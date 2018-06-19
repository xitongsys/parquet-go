package ParquetFile

import (
	"os"
)

type LocalFile struct {
	FilePath string
	File     *os.File
}

func NewLocalFileWriter(name string) (ParquetFile, error) {
	return (&LocalFile{}).Create(name)
}

func NewLocalFileReader(name string) (ParquetFile, error) {
	return (&LocalFile{}).Open(name)
}

func (self *LocalFile) Create(name string) (ParquetFile, error) {
	file, err := os.Create(name)
	myFile := new(LocalFile)
	myFile.FilePath = name
	myFile.File = file
	return myFile, err
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
func (self *LocalFile) Seek(offset int64, pos int) (int64, error) {
	return self.File.Seek(offset, pos)
}

func (self *LocalFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = self.File.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

func (self *LocalFile) Write(b []byte) (n int, err error) {
	return self.File.Write(b)
}

func (self *LocalFile) Close() error {
	return self.File.Close()
}
