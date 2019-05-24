package buffer

import (
	"bytes"

	"github.com/xitongsys/parquet-go/source"
)

// BufferFile allows reading parquet messages from a memory buffer.
type BufferFile struct {
	Reader *bytes.Reader
	Writer *bytes.Buffer
	buff   []byte
}

// NewBufferFile creates new in memory parquet buffer.
func NewBufferFile(b []byte) (source.ParquetFile, error) {
	return BufferFile{
		Reader: bytes.NewReader(b),
		Writer: bytes.NewBuffer(b),
		buff:   b,
	}, nil
}

func (bf BufferFile) Create(name string) (source.ParquetFile, error) {
	return BufferFile{
		Reader: bytes.NewReader(make([]byte, 0)),
		Writer: bytes.NewBuffer(make([]byte, 0)),
	}, nil
}

func (bf BufferFile) Open(name string) (source.ParquetFile, error) {
	return BufferFile{
		Reader: bytes.NewReader(bf.buff),
		Writer: bytes.NewBuffer(bf.buff),
	}, nil
}

// Seek seeks in the underlying memory buffer.
func (bf BufferFile) Seek(offset int64, pos int) (int64, error) {
	return bf.Reader.Seek(offset, pos)
}

// Read reads data form BufferFile into p.
func (bf BufferFile) Read(p []byte) (cnt int, err error) {
	var n int
	ln := len(p)
	for cnt < ln {
		n, err = bf.Reader.Read(p[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

// Write writes data from p into BufferFile.
func (bf BufferFile) Write(p []byte) (int, error) {
	n, err := bf.Writer.Write(p)
	return int(n), err
}

// Close is a no-op for a memory buffer.
func (bf BufferFile) Close() error {
	return nil
}

func (bf BufferFile) Bytes() []byte {
	return bf.Writer.Bytes()
}
