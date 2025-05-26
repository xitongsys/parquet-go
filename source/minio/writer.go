package minio

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *minioFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*minioWriter)(nil)

// minioFile is ParquetFileWriter for MinIO S3 API
type minioWriter struct {
	minioFile
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
}

// NewS3FileWriterWithClient is the same as NewMinioFileWriter but allows passing
// your own S3 client.
func NewS3FileWriterWithClient(ctx context.Context, s3Client *minio.Client, bucket, key string) (source.ParquetFileWriter, error) {
	file := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     s3Client,
			bucketName: bucket,
			key:        key,
		},
	}

	return file.Create(key)
}

// Write len(p) bytes from p to the Minio data stream
func (s *minioWriter) Write(p []byte) (n int, err error) {
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
func (s *minioWriter) Close() error {
	if s.pipeWriter != nil {
		if err := s.pipeWriter.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Create creates a new Minio File instance to perform writes
func (s *minioWriter) Create(key string) (source.ParquetFileWriter, error) {
	pf := &minioWriter{
		minioFile: minioFile{
			ctx:        s.ctx,
			client:     s.client,
			bucketName: s.bucketName,
			key:        key,
		},
	}
	pr, pw := io.Pipe()
	_, err := s.client.PutObject(s.ctx, s.bucketName, s.key, pr, -1, minio.PutObjectOptions{})
	if err != nil {
		return pf, err
	}
	pf.pipeReader = pr
	pf.pipeWriter = pw
	return pf, nil
}
