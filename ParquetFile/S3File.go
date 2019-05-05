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

// S3File is a TODO
type S3File struct {
	ctx    context.Context
	client s3iface.S3API
	offset int64
	whence int

	// write related fields
	writeOpened bool
	writeDone   chan struct{}
	pipeReader  *io.PipeReader
	pipeWriter  *io.PipeWriter
	downloader  *s3manager.Downloader

	// read related fields
	readOpened bool
	fileSize   int64

	BucketName string
	Key        string
}

var errWhence = errors.New("Seek: invalid whence")
var errOffset = errors.New("Seek: invalid offset")
var activeS3Session *session.Session

// NewS3FileWriter is TODO
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

// NewS3FileReader is TODO
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
			return 0, errOffset
		}
	case io.SeekCurrent:
		currentOffset := s.offset + offset
		if currentOffset < 0 || currentOffset > s.fileSize {
			return 0, errOffset
		}
	case io.SeekEnd:
		if offset > -1 || -offset > s.fileSize {
			return 0, errOffset
		}
	}

	s.offset = offset
	s.whence = whence
	return 0, nil
}

// Read up to len(p) bytes into p and return the number of bytes read.
// not safe for concurrent reads
func (s *S3File) Read(p []byte) (n int, err error) {
	if s.offset >= s.fileSize {
		return 0, io.EOF
	}

	numBytes := len(p)
	getObjRange := buildBytesRange(s.offset, s.whence, numBytes, s.fileSize)
	fmt.Printf("==== byte range %q\n", getObjRange)
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

	fmt.Printf("downloaded %d, new offset %d, %v\n", bytesDownloaded, s.offset, err)

	return int(bytesDownloaded), err
}

// Write is TODO
func (s *S3File) Write(p []byte) (n int, err error) {
	if !s.writeOpened {
		s.openWrite()
	}

	if s.pipeWriter != nil {
		// exit when writer is erroring
		return s.pipeWriter.Write(p)
	}

	return 0, nil
}

// Close is TODO
func (s *S3File) Close() error {
	s.offset = 0

	if s.pipeWriter != nil {
		s.pipeWriter.Close()
	}

	// if s.pipeReader != nil {
	// 	s.pipeReader.Close()
	// }

	// wait for pending uploads
	if s.writeDone != nil {
		<-s.writeDone
	}

	return nil
}

// Open is TODO reader
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

// Create is TODO
func (s *S3File) Create(name string) (ParquetFile, error) {
	s.offset = 0
	s.pipeReader = nil
	s.pipeWriter = nil

	return s, nil
}

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
		result, err := uploader.Upload(uploadParams)
		fmt.Printf("uploaded: %v %v\n", result, err)
		close(s.writeDone)
	}(uploader)
}

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

	fmt.Printf("opened file, size %d\n", s.fileSize)
	return nil
}

func buildBytesRange(offset int64, whence int, numBytes int, maxBytes int64) string {
	end := offset + int64(numBytes)
	if end > maxBytes {
		end = maxBytes
	}
	var byteRange string
	switch whence {
	case io.SeekStart:
		byteRange = fmt.Sprintf("bytes=%d-%d", offset, end)
	case io.SeekCurrent:
		byteRange = fmt.Sprintf("bytes=%d-%d", offset, end)
	case io.SeekEnd:
		byteRange = fmt.Sprintf("bytes=%d", offset)
	}

	return byteRange
}
