package gocloud

import (
	"context"

	"gocloud.dev/blob"
)

type blobFile struct {
	ctx    context.Context
	bucket *blob.Bucket
	key    string
	size   int64
}
