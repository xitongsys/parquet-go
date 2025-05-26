package gocloud

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"gocloud.dev/blob"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *blobFile implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*blobReader)(nil)

type blobReader struct {
	blobFile
	offset int64
}

func NewBlobReader(ctx context.Context, b *blob.Bucket, name string) (source.ParquetFileReader, error) {
	bf := &blobReader{
		blobFile: blobFile{
			ctx:    ctx,
			bucket: b,
		},
	}

	return bf.Open(name)
}

func (b *blobReader) Seek(offset int64, whence int) (int64, error) {
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

func (b *blobReader) Read(p []byte) (n int, err error) {
	r, err := b.bucket.NewRangeReader(b.ctx, b.key, b.offset, int64(len(p)), nil)
	if err != nil {
		return 0, errors.Wrapf(err, "Failed to open reader. key=%s, offset=%d, len=%d", b.key, b.offset, len(p))
	}
	defer func() {
		_ = r.Close()
	}()

	n, err = r.Read(p)
	b.offset += int64(n)

	return n, err
}

func (b *blobReader) Close() error {
	return nil
}

func (b *blobReader) Open(name string) (source.ParquetFileReader, error) {
	bf := &blobReader{
		blobFile: blobFile{
			ctx:    b.ctx,
			bucket: b.bucket,
		},
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

func (b blobReader) Clone() (source.ParquetFileReader, error) {
	return NewBlobReader(b.ctx, b.bucket, b.key)
}
