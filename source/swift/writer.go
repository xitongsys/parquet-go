package swiftsource

import (
	"github.com/ncw/swift"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *swiftFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*swiftWriter)(nil)

type swiftWriter struct {
	swiftFile
	fileWriter *swift.ObjectCreateFile
}

func NewSwiftFileWriter(container, filePath string, conn *swift.Connection) (source.ParquetFileWriter, error) {
	res := swiftWriter{
		swiftFile: swiftFile{
			connection: conn,
			container:  container,
			filePath:   filePath,
		},
	}

	return res.Create(filePath)
}

func (file *swiftWriter) Create(name string) (source.ParquetFileWriter, error) {
	fw, err := file.connection.ObjectCreate(file.container, name, false, "", "", nil)
	if err != nil {
		return nil, err
	}

	res := &swiftWriter{
		swiftFile: swiftFile{
			connection: file.connection,
			container:  file.container,
			filePath:   name,
		},
		fileWriter: fw,
	}

	return res, nil
}

func (file *swiftWriter) Write(p []byte) (n int, err error) {
	return file.fileWriter.Write(p)
}

func (file *swiftWriter) Close() error {
	if file.fileWriter != nil {
		if err := file.fileWriter.Close(); err != nil {
			return err
		}
	}
	return nil
}
