package gocloud

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob/memblob"
)

func TestSeek(t *testing.T) {
	bf := &blobFile{}

	// Out of range whence
	_, err := bf.Seek(0, io.SeekEnd+1)
	assert.Error(t, err)

	// Filesize is inconsequential for SeekStart and SeekCurrent
	_, err = bf.Seek(-1, io.SeekStart)
	assert.Error(t, err)

	offset, err := bf.Seek(10, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), offset)

	offset, err = bf.Seek(10, io.SeekCurrent)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), offset)

	offset, err = bf.Seek(-20, io.SeekCurrent)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), offset)

	_, err = bf.Seek(-1, io.SeekCurrent)
	assert.Error(t, err)

	// Ensure SeekEnd works correctly with zero sized files
	_, err = bf.Seek(-1, io.SeekEnd)
	assert.Error(t, err)

	offset, err = bf.Seek(1, io.SeekEnd)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), offset)

	// Ensure SeekEnd works correctly with non-zero file sizes
	bf.size = 1
	offset, err = bf.Seek(-1, io.SeekEnd)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), offset)

	_, err = bf.Seek(-2, io.SeekEnd)
	assert.Error(t, err)

	offset, err = bf.Seek(1, io.SeekEnd)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), offset)
}

func TestRead(t *testing.T) {
	b := memblob.OpenBucket(nil)
	defer func() {
		_ = b.Close()
	}()

	ctx := context.Background()
	key := "test"
	testData := []byte("test data")
	err := b.WriteAll(ctx, key, testData, nil)
	assert.NoError(t, err)

	bf, err := NewBlobReader(ctx, b, key)
	assert.NoError(t, err)

	buf := make([]byte, 1)
	n, err := bf.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, testData[:n], buf[:n])

	buf = make([]byte, 7)
	n, err = bf.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, testData[1:8], buf[:])

	buf = make([]byte, 7)
	n, err = bf.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, testData[8:], buf[:n])

	buf = make([]byte, 1)
	n, err = bf.Read(buf)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, n, 0)

	// Ensure Read operates as expected if we seek
	_, _ = bf.Seek(-1, io.SeekEnd)
	n, err = bf.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, testData[8:], buf[:n])

	n, err = bf.Read(buf)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, n, 0)
}

func TestWrite(t *testing.T) {
	b := memblob.OpenBucket(nil)
	defer func() {
		_ = b.Close()
	}()

	ctx := context.Background()
	key := "test"
	testData := []byte("test data")

	bf, err := NewBlobWriter(ctx, b, key)
	assert.NoError(t, err)

	n, err := bf.Write(testData)
	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)

	// All data is not guaranteed to exist prior to calling Close()
	err = bf.Close()
	assert.NoError(t, err)

	data, err := b.ReadAll(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, testData, data)

	// Opening an existing blob and writing to it replaces the contents
	bf, err = NewBlobWriter(ctx, b, key)
	assert.NoError(t, err)
	testOverwrite := []byte("overwritten")
	n, err = bf.Write(testOverwrite)
	assert.NoError(t, err)
	assert.Equal(t, len(testOverwrite), n)

	// All data is not guaranteed to exist prior to calling Close()
	err = bf.Close()
	assert.NoError(t, err)

	data, err = b.ReadAll(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, testOverwrite, data)

	// Don't write to things that don't exist
	bf = &blobFile{}
	n, err = bf.Write(testData)
	assert.Error(t, err)
	assert.Equal(t, 0, n)
}
