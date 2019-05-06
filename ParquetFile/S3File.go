package ParquetFile

//go:generate mockgen -destination=../mocks/mock_s3.go -package=mocks github.com/aws/aws-sdk-go/service/s3/s3iface S3API

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3File is ParquetFile for AWS S3
type S3File struct {
	ctx    context.Context
	client s3iface.S3API
	offset int64
	whence int

	// write-related fields
	writeOpened bool
	writeDone   chan struct{}
	pipeReader  *io.PipeReader
	pipeWriter  *io.PipeWriter
	downloader  *s3manager.Downloader

	// read-related fields
	readOpened bool
	fileSize   int64

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
)

// NewS3FileWriter creates an S3 FileWriter, to be used with NewParquetWriter
func NewS3FileWriter(ctx context.Context, bucket string, key string, cfgs ...*aws.Config) (ParquetFile, error) {
	if activeS3Session == nil {
		activeS3Session = session.Must(session.NewSession())
	}

	file := &S3File{
		ctx:        ctx,
		client:     s3.New(activeS3Session, cfgs...),
		writeDone:  make(chan struct{}),
		BucketName: bucket,
		Key:        key,
	}

	return file.Create(key)
}

// NewS3FileReader creates an S3 FileReader, to be used with NewParquetReader
func NewS3FileReader(ctx context.Context, bucket string, key string, cfgs ...*aws.Config) (ParquetFile, error) {
	if activeS3Session == nil {
		activeS3Session = session.Must(session.NewSession())
	}

	s3Client := s3.New(activeS3Session, cfgs...)
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
	switch whence {
	case io.SeekStart:
		if offset < 0 || offset > s.fileSize {
			return 0, errInvalidOffset
		}
	case io.SeekCurrent:
		currentOffset := s.offset + offset
		if currentOffset < 0 || currentOffset > s.fileSize {
			return 0, errInvalidOffset
		}
	case io.SeekEnd:
		if offset > -1 || -offset > s.fileSize {
			return 0, errInvalidOffset
		}
	}

	s.offset = offset
	s.whence = whence
	return 0, nil
}

// Read up to len(p) bytes into p and return the number of bytes read
func (s *S3File) Read(p []byte) (n int, err error) {
	if s.offset >= s.fileSize {
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
	bytesDownloaded, err := s.downloader.Download(wab, getObj)
	s.offset += bytesDownloaded
	if buf := wab.Bytes(); len(buf) > numBytes {
		// backing buffer reassigned, copy over some of the data
		copy(p, buf)
	}

	return int(bytesDownloaded), err
}

// Write len(p) bytes from p to the S3 data stream
func (s *S3File) Write(p []byte) (n int, err error) {
	if !s.writeOpened {
		s.openWrite()
	}

	if s.pipeWriter == nil {
		return 0, errFailedUpload
	}

	// prevent further writes upon error
	if _, err := s.pipeWriter.Write(p); err != nil {
		s.pipeWriter.CloseWithError(err)
		s.pipeWriter = nil
		return 0, err
	}

	return 0, nil
}

// Close cleans up any open streams. Will block until
// pending uploads are complete.
func (s *S3File) Close() error {
	s.offset = 0

	if s.pipeWriter != nil {
		s.pipeWriter.Close()
	}

	// wait for pending uploads
	if s.writeDone != nil {
		<-s.writeDone
	}

	if s.pipeReader != nil {
		s.pipeReader.Close()
	}

	return nil
}

// Open creates a new S3 File instance to perform concurrent reads
func (s *S3File) Open(name string) (ParquetFile, error) {
	if !s.readOpened {
		if err := s.openRead(); err != nil {
			return nil, err
		}
	}

	// create a new instance
	pf := &S3File{
		ctx:        s.ctx,
		client:     s.client,
		downloader: s.downloader,
		BucketName: s.BucketName,
		Key:        s.Key,
		readOpened: s.readOpened,
		fileSize:   s.fileSize,
		offset:     0,
	}
	return pf, nil
}

// Create creates a new S3 File instance to perform writes
func (s *S3File) Create(name string) (ParquetFile, error) {
	pf := &S3File{
		ctx:        s.ctx,
		client:     s.client,
		BucketName: s.BucketName,
		Key:        s.Key,
		writeDone:  make(chan struct{}),
	}
	return pf, nil
}

// openWrite creates an S3 uploader that consumes the Reader end of an io.Pipe.
// Calling Close signals write completion.
func (s *S3File) openWrite() {
	pr, pw := io.Pipe()
	s.pipeReader = pr
	s.pipeWriter = pw
	s.writeOpened = true

	uploadParams := &s3manager.UploadInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.Key),
		Body:   s.pipeReader,
	}
	uploader := s3manager.NewUploaderWithClient(s.client)

	go func(uploader *s3manager.Uploader) {
		// upload data and signal done when complete
		uploader.Upload(uploadParams)
		close(s.writeDone)
	}(uploader)
}

// openRead verifies the requested file is accessible and
// tracks the file size
func (s *S3File) openRead() error {
	hoi := &s3.HeadObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.Key),
	}

	hoo, err := s.client.HeadObject(hoi)
	if err != nil {
		return err
	}

	s.readOpened = true
	if hoo.ContentLength != nil {
		s.fileSize = *hoo.ContentLength
	}

	return nil
}

// getBytesRange returns the range request header string
func (s *S3File) getBytesRange(numBytes int) string {
	end := s.offset + int64(numBytes)
	if end > s.fileSize {
		end = s.fileSize
	}
	var byteRange string
	switch s.whence {
	case io.SeekStart:
		byteRange = fmt.Sprintf(rangeHeader, s.offset, end)
	case io.SeekCurrent:
		byteRange = fmt.Sprintf(rangeHeader, s.offset, end)
	case io.SeekEnd:
		byteRange = fmt.Sprintf(rangeHeaderSuffix, s.offset)
	}

	return byteRange
}
