package minio

import (
	"context"
	"errors"
	"io"

	"github.com/minio/minio-go/v7"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *minioFile implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*minioReader)(nil)

// minioFile is ParquetFileReader for MinIO S3 API
type minioReader struct {
	minioFile
	offset     int64
	fileSize   int64
	downloader *minio.Object
}

var (
	errWhence        = errors.New("Seek: invalid whence")
	errInvalidOffset = errors.New("Seek: invalid offset")
)

// NewS3FileReaderWithClient is the same as NewMinioFileReader but allows passing
// your own S3 client
func NewS3FileReaderWithClient(ctx context.Context, s3Client *minio.Client, bucket, key string) (source.ParquetFileReader, error) {
	file := &minioReader{
		minioFile: minioFile{
			ctx:        ctx,
			client:     s3Client,
			bucketName: bucket,
			key:        key,
		},
	}

	return file.Open(key)
}

// Seek tracks the offset for the next Read. Has no effect on Write.
func (s *minioReader) Seek(offset int64, whence int) (int64, error) {
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
	return s.offset, nil
}

// Read up to len(p) bytes into p and return the number of bytes read
func (s *minioReader) Read(p []byte) (n int, err error) {
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

// Close is a no-op
func (s *minioReader) Close() error {
	return nil
}

// Open creates a new Minio File instance to perform concurrent reads
func (s *minioReader) Open(name string) (source.ParquetFileReader, error) {
	// new instance
	pf := &minioReader{
		minioFile: minioFile{
			ctx:        s.ctx,
			client:     s.client,
			bucketName: s.bucketName,
			key:        name,
		},
		offset: 0,
	}
	// init object info
	downloader, err := s.client.GetObject(s.ctx, s.bucketName, s.key, minio.GetObjectOptions{})
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

func (s minioReader) Clone() (source.ParquetFileReader, error) {
	return NewS3FileReaderWithClient(s.ctx, s.client, s.bucketName, s.key)
}
