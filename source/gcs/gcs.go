package gcs

import (
	"context"

	"cloud.google.com/go/storage"
)

type gcsFile struct {
	projectID  string
	bucketName string
	filePath   string

	gcsClient      *storage.Client
	externalClient bool
	object         *storage.ObjectHandle
	ctx            context.Context //nolint:containedctx // Needed to create new readers and writers
}
