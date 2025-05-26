package local

import (
	"os"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *localFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*localWriter)(nil)

type localWriter struct {
	localFile
}

func NewLocalFileWriter(name string) (source.ParquetFileWriter, error) {
	return (&localWriter{}).Create(name)
}

func (f *localWriter) Create(name string) (source.ParquetFileWriter, error) {
	file, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	return &localWriter{
		localFile: localFile{
			filePath: name,
			file:     file,
		},
	}, nil
}

func (f *localWriter) Write(b []byte) (n int, err error) {
	return f.file.Write(b)
}

func (f *localWriter) Close() error {
	return f.file.Close()
}
