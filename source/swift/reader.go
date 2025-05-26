package swiftsource

import (
	"github.com/ncw/swift"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *swiftFile implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*swiftReader)(nil)

type swiftReader struct {
	swiftFile
	fileReader *swift.ObjectOpenFile
}

func NewSwiftFileReader(container, filePath string, conn *swift.Connection) (source.ParquetFileReader, error) {
	res := swiftReader{
		swiftFile: swiftFile{
			connection: conn,
			container:  container,
			filePath:   filePath,
		},
	}
	return res.Open(filePath)
}

func (file *swiftReader) Open(name string) (source.ParquetFileReader, error) {
	if name == "" {
		name = file.filePath
	}

	fr, _, err := file.connection.ObjectOpen(file.container, name, false, nil)
	if err != nil {
		return nil, err
	}

	res := &swiftReader{
		swiftFile: swiftFile{
			connection: file.connection,
			container:  file.container,
			filePath:   name,
		},
		fileReader: fr,
	}

	return res, nil
}

func (file *swiftReader) Read(b []byte) (n int, err error) {
	return file.fileReader.Read(b)
}

func (file *swiftReader) Seek(offset int64, whence int) (int64, error) {
	return file.fileReader.Seek(offset, whence)
}

func (file *swiftReader) Close() error {
	if file.fileReader != nil {
		if err := file.fileReader.Close(); err != nil {
			return err
		}
	}
	return nil
}
