package buffer

import (
	"errors"
	"io"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *bufferFile implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*bufferReader)(nil)

// bufferReader allows reading parquet messages from a memory buffer.
type bufferReader struct {
	bufferFile
}

// NewBufferReaderFromBytes creates new in memory parquet buffer from the given bytes.
// It allocates a new slice and copy the contents of s.
func NewBufferReaderFromBytes(s []byte) *bufferReader {
	b := make([]byte, len(s))
	copy(b, s)
	return NewBufferReaderFromBytesNoAlloc(b)
}

// NewBufferReaderFromBytesNoAlloc creates new in memory parquet buffer from the given bytes.
// It uses the provided slice as its buffer.
func NewBufferReaderFromBytesNoAlloc(s []byte) *bufferReader {
	return &bufferReader{
		bufferFile: bufferFile{buff: s},
	}
}

func (bf bufferReader) Open(string) (source.ParquetFileReader, error) {
	return NewBufferReaderFromBytes(bf.buff), nil
}

// Seek seeks in the underlying memory buffer.
func (bf *bufferReader) Seek(offset int64, whence int) (int64, error) {
	newLoc := bf.loc
	switch whence {
	case io.SeekStart:
		newLoc = int(offset)
	case io.SeekCurrent:
		newLoc += int(offset)
	case io.SeekEnd:
		newLoc = len(bf.buff) + int(offset)
	}

	if newLoc < 0 {
		return int64(bf.loc), errors.New("unable to seek to a location <0")
	}

	if newLoc > len(bf.buff) {
		newLoc = len(bf.buff)
	}

	bf.loc = newLoc

	return int64(bf.loc), nil
}

// Read reads data form BufferFile into p.
func (bf *bufferReader) Read(p []byte) (n int, err error) {
	n = copy(p, bf.buff[bf.loc:len(bf.buff)])
	bf.loc += n

	if bf.loc == len(bf.buff) {
		return n, io.EOF
	}

	return n, nil
}

// Close is a no-op for a memory buffer.
func (bf bufferReader) Close() error {
	return nil
}
