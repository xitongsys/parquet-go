package local

import (
	"os"

	"github.com/hangxie/parquet-go/v2/source"
)

type LocalFile struct {
	FilePath string
	File     *os.File
}

func NewLocalFileWriter(name string) (source.ParquetFileWriter, error) {
	return (&LocalFile{}).Create(name)
}

func NewLocalFileReader(name string) (source.ParquetFileReader, error) {
	return (&LocalFile{}).Open(name)
}

func (f *LocalFile) Create(name string) (source.ParquetFileWriter, error) {
	file, err := os.Create(name)
	myFile := new(LocalFile)
	myFile.FilePath = name
	myFile.File = file
	return myFile, err
}

func (f *LocalFile) Open(name string) (source.ParquetFileReader, error) {
	var err error
	if name == "" {
		name = f.FilePath
	}

	myFile := new(LocalFile)
	myFile.FilePath = name
	myFile.File, err = os.Open(name)
	return myFile, err
}

func (f *LocalFile) Seek(offset int64, pos int) (int64, error) {
	return f.File.Seek(offset, pos)
}

func (f *LocalFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = f.File.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

func (f *LocalFile) Write(b []byte) (n int, err error) {
	return f.File.Write(b)
}

func (f *LocalFile) Close() error {
	return f.File.Close()
}
