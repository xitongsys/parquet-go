package ParquetFile

import (
	"io"
)

type WriterFile struct {
	Writer io.Writer
}

func NewWriterFile(writer io.Writer) (ParquetFile, error) {
	return &WriterFile{Writer: writer}, nil
}

func (self *WriterFile) Create(name string) (ParquetFile, error) {
	return self, nil
}

func (self *WriterFile) Open(name string) (ParquetFile, error) {
	return self, nil
}

func (self *WriterFile) Seek(offset int, pos int) (int64, error) {
	return 0, nil
}

func (self *WriterFile) Read(b []byte) (int, error) {
	return 0, nil
}

func (self *WriterFile) Write(b []byte) (int, error) {
	return self.Writer.Write(b)
}

func (self *WriterFile) Close() {
}
