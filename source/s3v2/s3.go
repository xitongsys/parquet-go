package s3v2

import (
	"context"
	"sync"
)

type s3File struct {
	ctx        context.Context
	lock       sync.RWMutex
	err        error
	bucketName string
	key        string
}
