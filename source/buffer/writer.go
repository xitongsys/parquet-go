package buffer

import (
	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *bufferFile implement and source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*bufferWriter)(nil)

// bufferWriter allows reading parquet messages from a memory buffer.
type bufferWriter struct {
	bufferFile
}

// DefaultCapacity is the size in bytes of a new BufferFile's backing buffer
const DefaultCapacity = 512

// NewBufferWriter creates new in memory parquet buffer.
func NewBufferWriter() *bufferWriter {
	return NewBufferWriterCapacity(DefaultCapacity)
}

// NewBufferWriterCapacity starts the returned BufferFile with the given capacity
func NewBufferWriterCapacity(cap int) *bufferWriter {
	return &bufferWriter{
		bufferFile: bufferFile{buff: make([]byte, 0, cap)},
	}
}

// NewBufferWriterFromBytesNoAlloc creates new in memory parquet buffer from the given bytes.
// It uses the provided slice as its buffer.
func NewBufferWriterFromBytesNoAlloc(s []byte) *bufferWriter {
	return &bufferWriter{
		bufferFile: bufferFile{buff: s},
	}
}

func (bf bufferWriter) Create(string) (source.ParquetFileWriter, error) {
	return NewBufferWriter(), nil
}

// Write writes data from p into BufferFile.
func (bf *bufferWriter) Write(p []byte) (n int, err error) {
	// Do we have space?
	if available := cap(bf.buff) - bf.loc; available < len(p) {
		// How much should we expand by?
		addCap := cap(bf.buff)
		if addCap < len(p) {
			addCap = len(p)
		}

		newBuff := make([]byte, len(bf.buff), cap(bf.buff)+addCap)

		copy(newBuff, bf.buff)

		bf.buff = newBuff
	}

	// Write
	n = copy(bf.buff[bf.loc:cap(bf.buff)], p)
	bf.loc += n
	if len(bf.buff) < bf.loc {
		bf.buff = bf.buff[:bf.loc]
	}

	return n, nil
}

// Close is a no-op for a memory buffer.
func (bf bufferWriter) Close() error {
	return nil
}
