package gocloud

import (
	"context"

	"github.com/pkg/errors"
	"gocloud.dev/blob"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *blobFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*blobWriter)(nil)

type blobWriter struct {
	blobFile
	writer *blob.Writer
}

func NewBlobWriter(ctx context.Context, b *blob.Bucket, name string) (source.ParquetFileWriter, error) {
	bf := &blobWriter{
		blobFile: blobFile{
			ctx:    ctx,
			bucket: b,
		},
	}

	return bf.Create(name)
}

// Note that for blob storage, calling write on an existing blob overwrites that blob as opposed to appending to it.
// Additionally Write is not guaranteed to have succeeded unless Close() also succeeds
func (b *blobWriter) Write(p []byte) (n int, err error) {
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

func (b *blobWriter) Close() error {
	if b.writer != nil {
		return b.writer.Close()
	}

	return nil
}

func (b *blobWriter) Create(name string) (source.ParquetFileWriter, error) {
	if name == "" {
		return nil, errors.New("Parquet File name cannot be empty")
	}

	bf := &blobWriter{
		blobFile: blobFile{
			ctx:    b.ctx,
			bucket: b.bucket,
		},
	}

	w, err := bf.bucket.NewWriter(bf.ctx, name, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not create blob writer. blob=%s", name)
	}

	bf.key = name
	bf.writer = w

	return bf, nil
}
