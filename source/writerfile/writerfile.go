package writerfile

import (
	"io"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *writerFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*writerFile)(nil)

type writerFile struct {
	writer io.Writer
}

func NewWriterFile(writer io.Writer) source.ParquetFileWriter {
	return &writerFile{writer: writer}
}

func (f *writerFile) Create(name string) (source.ParquetFileWriter, error) {
	return f, nil
}

func (f *writerFile) Write(b []byte) (int, error) {
	return f.writer.Write(b)
}

func (f *writerFile) Close() error {
	return nil
}
