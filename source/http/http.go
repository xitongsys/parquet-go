package http

import (
	"errors"
	"github.com/xitongsys/parquet-go/source"
	"mime/multipart"
)

type MultipartFileWrapper struct {
	FH *multipart.FileHeader
	F  multipart.File
}

func NewMultipartFileWrapper(fh *multipart.FileHeader, f multipart.File) source.ParquetFile {
	return &MultipartFileWrapper{FH: fh, F: f}
}

func (mfw *MultipartFileWrapper) Create(_ string) (source.ParquetFile, error) {
	return nil, errors.New("cannot create a new multipart file")
}

// this method is called multiple times on one file to open parallel readers
func (mfw *MultipartFileWrapper) Open(_ string) (source.ParquetFile, error) {
	file, err := mfw.FH.Open()
	if err != nil {
		return nil, err
	}
	return NewMultipartFileWrapper(mfw.FH, file), nil
}

func (mfw *MultipartFileWrapper) Seek(offset int64, pos int) (int64, error) {
	return mfw.F.Seek(offset, pos)
}

func (mfw *MultipartFileWrapper) Read(p []byte) (int, error) {
	return mfw.F.Read(p)
}

func (mfw *MultipartFileWrapper) Write(_ []byte) (int, error) {
	return 0, errors.New("cannot write to request file")
}

func (mfw *MultipartFileWrapper) Close() error {
	return mfw.F.Close()
}
