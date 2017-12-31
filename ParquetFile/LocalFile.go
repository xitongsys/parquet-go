package ParquetFile

import (
	"os"
)

type LocalFile struct {
	FilePath string
	File     *os.File
}

func NewLocalFileWriter(name string) (ParquetFile, error) {
	file, err := os.Create(name)
	myFile := &LocalFile{
		FilePath: name,
		File:     file,
	}
	return myFile, err
}

func NewLocalFileReader(name string) (ParquetFile, error) {
	return (&LocalFile{}).Open(name)
}

func (self *LocalFile) Open(name string) (ParquetFile, error) {
	var (
		err error
	)
	if name == "" {
		name = self.FilePath
	}

	myFile := new(LocalFile)
	myFile.FilePath = name
	myFile.File, err = os.Open(name)
	return myFile, err
}
func (self *LocalFile) Seek(offset int, pos int) (int64, error) {
	return self.File.Seek(int64(offset), pos)
}

func (self *LocalFile) Read(b []byte) (n int, err error) {
	return self.File.Read(b)
}

func (self *LocalFile) Write(b []byte) (n int, err error) {
	return self.File.Write(b)
}

func (self *LocalFile) Close() {
	self.File.Close()
}
