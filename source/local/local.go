package local

import (
	"os"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *localFile implement the source.ParquetFileReader and source.ParquetFileWriter interface.
var (
	_ source.ParquetFileReader = (*localFile)(nil)
	_ source.ParquetFileWriter = (*localFile)(nil)
)

type localFile struct {
	filePath string
	file     *os.File
}

func NewLocalFileWriter(name string) (source.ParquetFileWriter, error) {
	return (&localFile{}).Create(name)
}

func NewLocalFileReader(name string) (source.ParquetFileReader, error) {
	return (&localFile{}).Open(name)
}

func (f *localFile) Create(name string) (source.ParquetFileWriter, error) {
	file, err := os.Create(name)
	myFile := new(localFile)
	myFile.filePath = name
	myFile.file = file
	return myFile, err
}

func (f *localFile) Open(name string) (source.ParquetFileReader, error) {
	var err error
	if name == "" {
		name = f.filePath
	}

	myFile := new(localFile)
	myFile.filePath = name
	myFile.file, err = os.Open(name)
	return myFile, err
}

func (f *localFile) Seek(offset int64, pos int) (int64, error) {
	return f.file.Seek(offset, pos)
}

func (f *localFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = f.file.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

func (f *localFile) Write(b []byte) (n int, err error) {
	return f.file.Write(b)
}

func (f *localFile) Close() error {
	return f.file.Close()
}
