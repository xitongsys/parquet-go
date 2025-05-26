package http

import (
	"mime/multipart"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *multipartFileWrapper implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*multipartFileReader)(nil)

type multipartFileReader struct {
	fileHeader *multipart.FileHeader
	file       multipart.File
}

func NewMultipartFileWrapper(fh *multipart.FileHeader, f multipart.File) source.ParquetFileReader {
	return &multipartFileReader{fileHeader: fh, file: f}
}

func (mfw *multipartFileReader) Open(_ string) (source.ParquetFileReader, error) {
	file, err := mfw.fileHeader.Open()
	if err != nil {
		return nil, err
	}
	return NewMultipartFileWrapper(mfw.fileHeader, file), nil
}

func (mfw multipartFileReader) Clone() (source.ParquetFileReader, error) {
	return NewMultipartFileWrapper(mfw.fileHeader, mfw.file), nil
}

func (mfw *multipartFileReader) Seek(offset int64, pos int) (int64, error) {
	return mfw.file.Seek(offset, pos)
}

func (mfw *multipartFileReader) Read(p []byte) (int, error) {
	return mfw.file.Read(p)
}

func (mfw *multipartFileReader) Close() error {
	return mfw.file.Close()
}
