package s3v2

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hangxie/parquet-go/v2/source"
)

type s3WriteClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// Compile time check that *s3File implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*s3Writer)(nil)

type s3Writer struct {
	s3File
	client                s3WriteClient
	writeOpened           bool
	writeDone             chan error
	pipeReader            *io.PipeReader
	pipeWriter            *io.PipeWriter
	uploader              *manager.Uploader
	uploaderOptions       []func(*manager.Uploader)
	putObjectInputOptions []func(*s3.PutObjectInput)
}

// NewS3FileWriter creates an S3 FileWriter, to be used with NewParquetWriter
func NewS3FileWriter(ctx context.Context, bucket, key string, uploaderOptions []func(*manager.Uploader), cfgs ...*aws.Config) (source.ParquetFileWriter, error) {
	return NewS3FileWriterWithClient(
		ctx,
		s3.NewFromConfig(getConfig()),
		bucket,
		key,
		uploaderOptions,
	)
}

// NewS3FileWriterWithClient is the same as NewS3FileWriter but allows passing
// your own S3 client.
func NewS3FileWriterWithClient(ctx context.Context, s3Client s3WriteClient, bucket, key string, uploaderOptions []func(*manager.Uploader), putObjectInputOptions ...func(*s3.PutObjectInput)) (source.ParquetFileWriter, error) {
	file := &s3Writer{
		s3File: s3File{
			ctx:        ctx,
			bucketName: bucket,
			key:        key,
		},
		client:                s3Client,
		writeDone:             make(chan error),
		uploaderOptions:       uploaderOptions,
		putObjectInputOptions: putObjectInputOptions,
	}

	return file.Create(key)
}

// Write len(p) bytes from p to the S3 data stream
func (s *s3Writer) Write(p []byte) (n int, err error) {
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
func (s *s3Writer) Close() error {
	if s.pipeWriter != nil {
		if err := s.pipeWriter.Close(); err != nil {
			return err
		}
	}

	// wait for pending uploads
	if s.writeDone == nil {
		return nil
	}

	return <-s.writeDone
}

// Create creates a new S3 File instance to perform writes
func (s *s3Writer) Create(key string) (source.ParquetFileWriter, error) {
	pf := &s3Writer{
		s3File: s3File{
			ctx:        s.ctx,
			bucketName: s.bucketName,
			key:        key,
		},
		client:                s.client,
		uploaderOptions:       s.uploaderOptions,
		putObjectInputOptions: s.putObjectInputOptions,
		writeDone:             make(chan error),
	}
	pf.openWrite()
	return pf, nil
}

// openWrite creates an S3 uploader that consumes the Reader end of an io.Pipe.
// Calling Close signals write completion.
func (s *s3Writer) openWrite() {
	pr, pw := io.Pipe()
	uploader := manager.NewUploader(s.client, s.uploaderOptions...)
	s.lock.Lock()
	s.pipeReader = pr
	s.pipeWriter = pw
	s.writeOpened = true
	s.uploader = uploader
	s.lock.Unlock()

	uploadParams := &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s.key),
		Body:   s.pipeReader,
	}

	for _, f := range s.putObjectInputOptions {
		f(uploadParams)
	}

	go func(uploader *manager.Uploader, params *s3.PutObjectInput, done chan error) {
		defer close(done)

		// upload data and signal done when complete
		_, err := uploader.Upload(s.ctx, params)
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

// configured using the S3FileReaderParams object.
func NewS3FileReaderWithParams(ctx context.Context, params S3FileReaderParams) (source.ParquetFileReader, error) {
	s3Client := params.S3Client
	if s3Client == nil {
		s3Client = s3.NewFromConfig(getConfig())
	}

	minRequestSize := int64(params.MinRequestSize)
	if minRequestSize == 0 {
		minRequestSize = defaultMinRequestSize
	}

	file := &s3Reader{
		s3File: s3File{
			ctx:        ctx,
			bucketName: params.Bucket,
			key:        params.Key,
		},
		client:         s3Client,
		version:        params.Version,
		minRequestSize: minRequestSize,
	}

	return file.Open(params.Key)
}
