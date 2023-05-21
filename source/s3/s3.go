package s3

//go:generate mockgen -destination=./mocks/mock_s3.go -package=mocks github.com/aws/aws-sdk-go/service/s3/s3iface S3API

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
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
	readOpened     bool
	fileSize       int64
	socket         io.ReadCloser
	minRequestSize int64

	lock       sync.RWMutex
	err        error
	BucketName string
	Key        string
	VersionId  *string
	ACL        string
}

const (
	rangeHeader                 = "bytes=%d-%d"
	rangeHeaderSuffix           = "bytes=%d"
	defaultMinRequestSize int64 = math.MaxUint32
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
	acl string,
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
		ctx, s3.New(activeS3Session, cfgs...), bucket, key, acl, uploaderOptions)
}

// NewS3FileWriterWithClient is the same as NewS3FileWriter but allows passing
// your own S3 client.
func NewS3FileWriterWithClient(
	ctx context.Context,
	s3Client s3iface.S3API,
	bucket string,
	key string,
	acl string,
	uploaderOptions []func(*s3manager.Uploader),
) (source.ParquetFile, error) {
	file := &S3File{
		ctx:             ctx,
		client:          s3Client,
		writeDone:       make(chan error),
		uploaderOptions: uploaderOptions,
		BucketName:      bucket,
		Key:             key,
		ACL:             acl,
	}

	return file.Create(key)
}

// NewS3FileReader creates an S3 FileReader, to be used with NewParquetReader
func NewS3FileReader(ctx context.Context, bucket string, key string, cfgs ...*aws.Config) (source.ParquetFile, error) {
	return NewS3FileReaderWithParams(ctx, S3FileReaderParams{
		Bucket:  bucket,
		Key:     key,
		Configs: cfgs,
	})
}

// NewS3FileReaderWithClient is the same as NewS3FileReader but allows passing
// your own S3 client
func NewS3FileReaderWithClient(ctx context.Context, s3Client s3iface.S3API, bucket string, key string) (source.ParquetFile, error) {
	return NewS3FileReaderWithParams(ctx, S3FileReaderParams{
		Bucket:   bucket,
		Key:      key,
		S3Client: s3Client,
	})
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
			if offset == 0 {
				offset = s.fileSize
			} else if offset > 0 || -offset > s.fileSize {
				return 0, errInvalidOffset
			}
		}
	}

	s.offset = offset
	s.whence = whence

	s.closeSocket()
	return s.offset, nil
}

// Read up to len(p) bytes into p and return the number of bytes read
func (s *S3File) Read(p []byte) (n int, err error) {
	if s.fileSize > 0 && s.offset >= s.fileSize {
		return 0, io.EOF
	}

	defer func() {
		if err != nil {
			s.closeSocket()
		}
	}()

	if s.socket == nil {
		err = s.openSocket(int64(len(p)))
		if err != nil {
			return 0, err
		}
	}

	n, err = s.socket.Read(p)
	// Because the chunk size is not infinite, we might hit the end of the socket while
	// there's still data in the file. In this case, we close the socket so that the next
	// read call will request a new one, and we return a nil error so that the caller
	// will not think the file is done.
	if err == io.EOF {
		err = nil
		s.closeSocket()
	}
	s.offset += int64(n)

	return n, err
}

// openSocket issues a new GetObject request to retrieve the next chunk of data from the
// object.
func (s *S3File) openSocket(numBytes int64) error {
	if numBytes < s.minRequestSize {
		numBytes = s.minRequestSize
	}
	getObjRange := s.getBytesRange(numBytes)
	getObj := &s3.GetObjectInput{
		Bucket:    aws.String(s.BucketName),
		Key:       aws.String(s.Key),
		VersionId: s.VersionId,
	}
	if len(getObjRange) > 0 {
		getObj.Range = aws.String(getObjRange)
	}
	out, err := s.client.GetObjectWithContext(s.ctx, getObj)
	if err != nil {
		return err
	}
	s.socket = out.Body
	return nil
}

func (s *S3File) closeSocket() {
	if s.socket != nil {
		s.socket.Close()
		s.socket = nil
	}
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

	s.closeSocket()

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
		ctx:            s.ctx,
		client:         s.client,
		BucketName:     s.BucketName,
		Key:            name,
		VersionId:      s.VersionId,
		readOpened:     s.readOpened,
		fileSize:       s.fileSize,
		minRequestSize: s.minRequestSize,
		offset:         0,
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
		ACL:             s.ACL,
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
		ACL:    aws.String(s.ACL),
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
		Bucket:    aws.String(s.BucketName),
		Key:       aws.String(s.Key),
		VersionId: s.VersionId,
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
func (s *S3File) getBytesRange(numBytes int64) string {
	var (
		byteRange string
		begin     int64
		end       int64
	)

	// Processing for unknown file size relies on the requester to
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

// NewS3FileReaderVersioned creates an S3 FileReader for a versioned of S3 object, to be used with NewParquetReader
func NewS3FileReaderVersioned(ctx context.Context, bucket string, key string, version *string, cfgs ...*aws.Config) (source.ParquetFile, error) {
	return NewS3FileReaderWithParams(ctx, S3FileReaderParams{
		Bucket:  bucket,
		Key:     key,
		Configs: cfgs,
		Version: version,
	})
}

// NewS3FileReaderVersionedWithClient is the same as NewS3FileReaderVersioned but allows passing
// your own S3 client
func NewS3FileReaderVersionedWithClient(ctx context.Context, s3Client s3iface.S3API, bucket string, key string, version *string) (source.ParquetFile, error) {
	return NewS3FileReaderWithParams(ctx, S3FileReaderParams{
		Bucket:   bucket,
		Key:      key,
		S3Client: s3Client,
		Version:  version,
	})
}

// S3FileReaderParams contains fields used to initialize and configure an S3File object
// for reading.
type S3FileReaderParams struct {
	Bucket string
	Key    string

	// S3Client will be used to issue requests to S3. If not set, a new one one will be
	// created. Optional.
	S3Client s3iface.S3API
	// Configs are the configs used to construct a new S3Client. If S3Client is provided,
	// Configs are ignored. Optional.
	Configs []*aws.Config
	// Version is the version of the S3 object that will be read. If not set, the newest
	// version will be read. Optional.
	Version *string
	// MinRequestSize controls the amount of data per request that the S3File will ask for
	// from S3. Optional.
	// A large MinRequestSize should improve performance and reduce AWS costs due to
	// number of request. However, in some cases it may increase AWS costs due to data
	// processing or data transfer. For best results, set it at or above the largest of
	// the footer size and the biggest chunk size in the parquet file.
	// S3File will not buffer a large amount of data in memory at one time, regardless
	// of the value of MinRequestSize.
	MinRequestSize int
}

// NewS3FileReaderWithParams creates an S3 FileReader for an object identified by and
// configured using the S3FileReaderParams object.
func NewS3FileReaderWithParams(ctx context.Context, params S3FileReaderParams) (source.ParquetFile, error) {
	s3Client := params.S3Client
	if s3Client == nil {
		if activeS3Session == nil {
			sessLock.Lock()
			if activeS3Session == nil {
				activeS3Session = session.Must(session.NewSession())
			}
			sessLock.Unlock()
		}
		s3Client = s3.New(activeS3Session, params.Configs...)
	}

	minRequestSize := int64(params.MinRequestSize)
	if minRequestSize == 0 {
		minRequestSize = defaultMinRequestSize
	}

	file := &S3File{
		ctx:            ctx,
		client:         s3Client,
		BucketName:     params.Bucket,
		Key:            params.Key,
		VersionId:      params.Version,
		minRequestSize: minRequestSize,
	}

	return file.Open(params.Key)
}
