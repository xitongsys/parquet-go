package s3

//go:generate mockgen -destination=../mocks/mock_s3.go -package=mocks github.com/aws/aws-sdk-go/service/s3/s3iface S3API

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go/source"
)

// S3File is ParquetFile for AWS S3
type S3File struct {
	ctx    context.Context
	client s3iface.S3API
	offset int64
	whence int

	// write-related fields
	writeOpened     bool
	writeDone       chan error
	pipeReader      *io.PipeReader
	pipeWriter      *io.PipeWriter
	uploader        *s3manager.Uploader
	uploaderOptions []func(*s3manager.Uploader)

	// read-related fields
	readOpened bool
	fileSize   int64
	downloader *s3manager.Downloader

	lock       sync.RWMutex
	err        error
	BucketName string
	Key        string
}

const (
	rangeHeader       = "bytes=%d-%d"
	rangeHeaderSuffix = "bytes=%d"
)

var (
	errWhence        = errors.New("Seek: invalid whence")
	errInvalidOffset = errors.New("Seek: invalid offset")
	errFailedUpload  = errors.New("Write: failed upload")
	activeS3Session  *session.Session
	sessLock         sync.Mutex
)

// SetActiveSession sets the current session. If this is unset, the functions
// of this package will implicitly create a new session.Session and use that.
// This allows you to control what session is used, particularly useful for
// testing with a system like localstack.
func SetActiveSession(sess *session.Session) {
	sessLock.Lock()
	activeS3Session = sess
	sessLock.Unlock()
}

// NewS3FileWriter creates an S3 FileWriter, to be used with NewParquetWriter
func NewS3FileWriter(
	ctx context.Context,
	bucket string,
	key string,
	uploaderOptions []func(*s3manager.Uploader),
	cfgs ...*aws.Config,
) (source.ParquetFile, error) {
	if activeS3Session == nil {
		sessLock.Lock()
		if activeS3Session == nil {
			activeS3Session = session.Must(session.NewSession())
		}
		sessLock.Unlock()
	}

	return NewS3FileWriterWithClient(
		ctx, s3.New(activeS3Session, cfgs...), bucket, key, uploaderOptions)
}

// NewS3FileWriterWithClient is the same as NewS3FileWriter but allows passing
// your own S3 client.
func NewS3FileWriterWithClient(
	ctx context.Context,
	s3Client s3iface.S3API,
	bucket string,
	key string,
	uploaderOptions []func(*s3manager.Uploader),
) (source.ParquetFile, error) {
	file := &S3File{
		ctx:             ctx,
		client:          s3Client,
		writeDone:       make(chan error),
		uploaderOptions: uploaderOptions,
		BucketName:      bucket,
		Key:             key,
	}

	return file.Create(key)
}

// NewS3FileReader creates an S3 FileReader, to be used with NewParquetReader
func NewS3FileReader(ctx context.Context, bucket string, key string, cfgs ...*aws.Config) (source.ParquetFile, error) {
	if activeS3Session == nil {
		sessLock.Lock()
		if activeS3Session == nil {
			activeS3Session = session.Must(session.NewSession())
		}
		sessLock.Unlock()
	}

	return NewS3FileReaderWithClient(ctx, s3.New(activeS3Session, cfgs...), bucket, key)
}

// NewS3FileReaderWithClient is the same as NewS3FileReader but allows passing
// your own S3 client
func NewS3FileReaderWithClient(ctx context.Context, s3Client s3iface.S3API, bucket string, key string) (source.ParquetFile, error) {
	s3Downloader := s3manager.NewDownloaderWithClient(s3Client)

	file := &S3File{
		ctx:        ctx,
		client:     s3Client,
		downloader: s3Downloader,
		BucketName: bucket,
		Key:        key,
	}

	return file.Open(key)
}

// Seek tracks the offset for the next Read. Has no effect on Write.
func (s *S3File) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errWhence
	}

	if s.fileSize > 0 {
		switch whence {
		case io.SeekStart:
			if offset < 0 || offset > s.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekCurrent:
			offset += s.offset
			if offset < 0 || offset > s.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekEnd:
			if offset > -1 || -offset > s.fileSize {
				return 0, errInvalidOffset
			}
		}
	}

	s.offset = offset
	s.whence = whence
	return s.offset, nil
}

// Read up to len(p) bytes into p and return the number of bytes read
func (s *S3File) Read(p []byte) (n int, err error) {
	if s.fileSize > 0 && s.offset >= s.fileSize {
		return 0, io.EOF
	}

	numBytes := len(p)
	getObjRange := s.getBytesRange(numBytes)
	getObj := &s3.GetObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.Key),
	}
	if len(getObjRange) > 0 {
		getObj.Range = aws.String(getObjRange)
	}

	wab := aws.NewWriteAtBuffer(p)
	bytesDownloaded, err := s.downloader.DownloadWithContext(s.ctx, wab, getObj)
	if err != nil {
		return 0, err
	}

	s.offset += bytesDownloaded
	if buf := wab.Bytes(); len(buf) > numBytes {
		// backing buffer reassigned, copy over some of the data
		copy(p, buf)
		bytesDownloaded = int64(len(p))
	}

	return int(bytesDownloaded), err
}

// Write len(p) bytes from p to the S3 data stream
func (s *S3File) Write(p []byte) (n int, err error) {
	s.lock.RLock()
	writeOpened := s.writeOpened
	s.lock.RUnlock()
	if !writeOpened {
		s.openWrite()
	}

	s.lock.RLock()
	writeError := s.err
	s.lock.RUnlock()
	if writeError != nil {
		return 0, writeError
	}

	// prevent further writes upon error
	bytesWritten, writeError := s.pipeWriter.Write(p)
	if writeError != nil {
		s.lock.Lock()
		s.err = writeError
		s.lock.Unlock()

		s.pipeWriter.CloseWithError(err)
		return 0, writeError
	}

	return bytesWritten, nil
}

// Close signals write completion and cleans up any
// open streams. Will block until pending uploads are complete.
func (s *S3File) Close() error {
	var err error

	if s.pipeWriter != nil {
		if err = s.pipeWriter.Close(); err != nil {
			return err
		}
	}

	// wait for pending uploads
	if s.writeDone != nil {
		err = <-s.writeDone
	}

	return err
}

// Open creates a new S3 File instance to perform concurrent reads
func (s *S3File) Open(name string) (source.ParquetFile, error) {
	s.lock.RLock()
	readOpened := s.readOpened
	s.lock.RUnlock()
	if !readOpened {
		if err := s.openRead(); err != nil {
			return nil, err
		}
	}

	// ColumBuffer passes in an empty string for name
	if len(name) == 0 {
		name = s.Key
	}

	// create a new instance
	pf := &S3File{
		ctx:        s.ctx,
		client:     s.client,
		downloader: s.downloader,
		BucketName: s.BucketName,
		Key:        name,
		readOpened: s.readOpened,
		fileSize:   s.fileSize,
		offset:     0,
	}
	return pf, nil
}

// Create creates a new S3 File instance to perform writes
func (s *S3File) Create(key string) (source.ParquetFile, error) {
	pf := &S3File{
		ctx:             s.ctx,
		client:          s.client,
		uploaderOptions: s.uploaderOptions,
		BucketName:      s.BucketName,
		Key:             key,
		writeDone:       make(chan error),
	}
	pf.openWrite()
	return pf, nil
}

// openWrite creates an S3 uploader that consumes the Reader end of an io.Pipe.
// Calling Close signals write completion.
func (s *S3File) openWrite() {
	pr, pw := io.Pipe()
	uploader := s3manager.NewUploaderWithClient(s.client, s.uploaderOptions...)
	s.lock.Lock()
	s.pipeReader = pr
	s.pipeWriter = pw
	s.writeOpened = true
	s.uploader = uploader
	s.lock.Unlock()

	uploadParams := &s3manager.UploadInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.Key),
		Body:   s.pipeReader,
	}

	go func(uploader *s3manager.Uploader, params *s3manager.UploadInput, done chan error) {
		defer close(done)

		// upload data and signal done when complete
		_, err := uploader.UploadWithContext(s.ctx, params)
		if err != nil {
			s.lock.Lock()
			s.err = err
			s.lock.Unlock()

			if s.writeOpened {
				s.pipeWriter.CloseWithError(err)
			}
		}

		done <- err
	}(s.uploader, uploadParams, s.writeDone)
}

// openRead verifies the requested file is accessible and
// tracks the file size
func (s *S3File) openRead() error {
	hoi := &s3.HeadObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.Key),
	}

	hoo, err := s.client.HeadObjectWithContext(s.ctx, hoi)
	if err != nil {
		return err
	}

	s.lock.Lock()
	s.readOpened = true
	if hoo.ContentLength != nil {
		s.fileSize = *hoo.ContentLength
	}
	s.lock.Unlock()

	return nil
}

// getBytesRange returns the range request header string
func (s *S3File) getBytesRange(numBytes int) string {
	var (
		byteRange string
		begin     int64
		end       int64
	)

	// Processing for unknown file size relies on the requestor to
	// know which ranges are valid. May occur if caller is missing HEAD permissions.
	if s.fileSize < 1 {
		switch s.whence {
		case io.SeekStart, io.SeekCurrent:
			byteRange = fmt.Sprintf(rangeHeader, s.offset, s.offset+int64(numBytes)-1)
		case io.SeekEnd:
			byteRange = fmt.Sprintf(rangeHeaderSuffix, s.offset)
		}
		return byteRange
	}

	switch s.whence {
	case io.SeekStart, io.SeekCurrent:
		begin = s.offset
	case io.SeekEnd:
		begin = s.fileSize + s.offset
	default:
		return byteRange
	}

	endIndex := s.fileSize - 1
	if begin < 0 {
		begin = 0
	}
	end = begin + int64(numBytes) - 1
	if end > endIndex {
		end = endIndex
	}

	byteRange = fmt.Sprintf(rangeHeader, begin, end)
	return byteRange
}
