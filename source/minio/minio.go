package minio

import (
	"context"
	"errors"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/xitongsys/parquet-go/source"
)

// MinioFile is ParquetFile for MinIO S3 API
type MinioFile struct {
	ctx    context.Context
	client *minio.Client
	offset int64
	whence int

	// write-related fields
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter

	// read-related fields
	fileSize   int64
	downloader *minio.Object

	err        error
	BucketName string
	Key        string
}

var (
	errWhence        = errors.New("Seek: invalid whence")
	errInvalidOffset = errors.New("Seek: invalid offset")
	errFailedUpload  = errors.New("Write: failed upload")
)

// NewS3FileWriterWithClient is the same as NewMinioFileWriter but allows passing
// your own S3 client.
func NewS3FileWriterWithClient(
	ctx context.Context,
	s3Client *minio.Client,
	bucket string,
	key string,
) (source.ParquetFile, error) {
	file := &MinioFile{
		ctx:        ctx,
		client:     s3Client,
		BucketName: bucket,
		Key:        key,
	}

	return file.Create(key)
}

// NewS3FileReaderWithClient is the same as NewMinioFileReader but allows passing
// your own S3 client
func NewS3FileReaderWithClient(ctx context.Context, s3Client *minio.Client, bucket string, key string) (source.ParquetFile, error) {
	file := &MinioFile{
		ctx:        ctx,
		client:     s3Client,
		BucketName: bucket,
		Key:        key,
	}

	return file.Open(key)
}

// Seek tracks the offset for the next Read. Has no effect on Write.
func (s *MinioFile) Seek(offset int64, whence int) (int64, error) {
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
func (s *MinioFile) Read(p []byte) (n int, err error) {
	if s.fileSize > 0 && s.offset >= s.fileSize {
		return 0, io.EOF
	}

	bytesDownloaded, err := s.downloader.ReadAt(p, s.offset)
	if err != nil {
		return 0, err
	}

	s.offset += int64(bytesDownloaded)
	return bytesDownloaded, err
}

// Write len(p) bytes from p to the Minio data stream
func (s *MinioFile) Write(p []byte) (n int, err error) {
	// prevent further writes upon error
	bytesWritten, writeError := s.pipeWriter.Write(p)
	if writeError != nil {
		s.err = writeError
		s.pipeWriter.CloseWithError(err)
		return 0, writeError
	}

	return bytesWritten, nil
}

// Close signals write completion and cleans up any
// open streams. Will block until pending uploads are complete.
func (s *MinioFile) Close() error {
	var err error

	if s.pipeWriter != nil {
		if err = s.pipeWriter.Close(); err != nil {
			return err
		}
	}

	return err
}

// Open creates a new Minio File instance to perform concurrent reads
func (s *MinioFile) Open(name string) (source.ParquetFile, error) {
	// new instance
	pf := &MinioFile{
		ctx:        s.ctx,
		client:     s.client,
		BucketName: s.BucketName,
		Key:        name,
		offset:     0,
	}
	// init object info
	downloader, err := s.client.GetObject(s.ctx, s.BucketName, s.Key, minio.GetObjectOptions{})
	if err != nil {
		return pf, err
	}
	info, err := downloader.Stat()
	if err != nil {
		return pf, err
	}
	s.downloader = downloader
	s.fileSize = info.Size
	return pf, nil
}

// Create creates a new Minio File instance to perform writes
func (s *MinioFile) Create(key string) (source.ParquetFile, error) {
	pf := &MinioFile{
		ctx:        s.ctx,
		client:     s.client,
		BucketName: s.BucketName,
		Key:        key,
	}
	pr, pw := io.Pipe()
	_, err := s.client.PutObject(s.ctx, s.BucketName, s.Key, pr, -1, minio.PutObjectOptions{})
	if err != nil {
		return pf, err
	}
	pf.pipeReader = pr
	pf.pipeWriter = pw
	return pf, nil
}
