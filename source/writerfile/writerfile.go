package writerfile

import (
	"io"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *SwiftFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*WriterFile)(nil)

type WriterFile struct {
	Writer io.Writer
}

func NewWriterFile(writer io.Writer) source.ParquetFileWriter {
	return &WriterFile{Writer: writer}
}

func (f *WriterFile) Create(name string) (source.ParquetFileWriter, error) {
	return f, nil
}

func (f *WriterFile) Write(b []byte) (int, error) {
	return f.Writer.Write(b)
}

func (f *WriterFile) Close() error {
	return nil
}
