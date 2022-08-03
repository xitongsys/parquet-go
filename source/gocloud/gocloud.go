package gocloud

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go/source"
	"gocloud.dev/blob"
)

type blobFile struct {
	ctx    context.Context
	bucket *blob.Bucket
	writer *blob.Writer

	key    string
	size   int64
	offset int64
}

func NewBlobWriter(ctx context.Context, b *blob.Bucket, name string) (source.ParquetFile, error) {
	bf := &blobFile{
		ctx:    ctx,
		bucket: b,
	}

	return bf.Create(name)
}

func NewBlobReader(ctx context.Context, b *blob.Bucket, name string) (source.ParquetFile, error) {
	bf := &blobFile{
		ctx:    ctx,
		bucket: b,
	}

	return bf.Open(name)
}

func (b *blobFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += b.offset
	case io.SeekEnd:
		offset = b.size + offset
	default:
		return 0, errors.Errorf("Invalid whence. whence=%d", whence)
	}

	if offset < 0 {
		return 0, errors.Errorf("Invalid offset. offset=%d", offset)
	}

	b.offset = offset

	return b.offset, nil
}

func (b *blobFile) Read(p []byte) (n int, err error) {
	r, err := b.bucket.NewRangeReader(b.ctx, b.key, b.offset, int64(len(p)), nil)
	if err != nil {
		return 0, errors.Wrapf(err, "Failed to open reader. key=%s, offset=%d, len=%d", b.key, b.offset, len(p))
	}
	defer r.Close()

	n, err = r.Read(p)
	b.offset += int64(n)

	return n, err
}

// Note that for blob storage, calling write on an existing blob overwrites that blob as opposed to appending to it.
// Additionally Write is not guaranteed to have succeeded unless Close() also succeeds
func (b *blobFile) Write(p []byte) (n int, err error) {
	if b.writer == nil {
		if b.key == "" {
			return 0, errors.New("Invalid call to write, you must create or open a ParquetFile for writing")
		}

		if w, err := b.bucket.NewWriter(b.ctx, b.key, nil); err != nil {
			return 0, errors.Wrapf(err, "Could not create blob writer. key=%s", b.key)
		} else {
			b.writer = w
		}
	}

	n, err = b.writer.Write(p)
	b.size += int64(n)

	return n, err
}

func (b *blobFile) Close() error {
	if b.writer != nil {
		return b.writer.Close()
	}

	return nil
}

func (b *blobFile) Create(name string) (source.ParquetFile, error) {
	if name == "" {
		return nil, errors.New("Parquet File name cannot be empty")
	}

	bf := &blobFile{
		ctx:    b.ctx,
		bucket: b.bucket,
	}

	w, err := bf.bucket.NewWriter(bf.ctx, name, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not create blob writer. blob=%s", name)
	}

	bf.key = name
	bf.writer = w

	return bf, nil
}

func (b *blobFile) Open(name string) (source.ParquetFile, error) {
	bf := &blobFile{
		ctx:    b.ctx,
		bucket: b.bucket,
	}

	if name == "" {
		name = b.key
	}
	if e, err := bf.bucket.Exists(bf.ctx, name); !e || err != nil {
		return nil, errors.Errorf("Requested blob does not exist. blob=%s", name)
	}

	bf.key = name
	attrs, err := bf.bucket.Attributes(bf.ctx, bf.key)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get attributes for blob. blob=%s", name)
	}

	bf.size = attrs.Size
	return bf, nil
}
