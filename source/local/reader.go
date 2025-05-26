package local

import (
	"os"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *localFile implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*localReader)(nil)

type localReader struct {
	localFile
}

func NewLocalFileReader(name string) (source.ParquetFileReader, error) {
	return (&localReader{}).Open(name)
}

func (f *localReader) Open(name string) (source.ParquetFileReader, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	return &localReader{
		localFile: localFile{
			filePath: name,
			file:     file,
		},
	}, nil
}

func (f localReader) Clone() (source.ParquetFileReader, error) {
	return NewLocalFileReader(f.filePath)
}

func (f *localReader) Seek(offset int64, pos int) (int64, error) {
	return f.file.Seek(offset, pos)
}

func (f *localReader) Read(b []byte) (cnt int, err error) {
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

func (f *localReader) Close() error {
	return f.file.Close()
}
