package minio

import (
	"context"

	"github.com/minio/minio-go/v7"
)

// minioFile is ParquetFile for MinIO S3 API
type minioFile struct {
	ctx        context.Context
	client     *minio.Client
	bucketName string
	key        string
	err        error
}
