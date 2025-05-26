package source

import (
	"io"

	"github.com/apache/thrift/lib/go/thrift"
)

type ParquetFileReader interface {
	io.Seeker
	io.Reader
	io.Closer
	Open(name string) (ParquetFileReader, error)
	Clone() (ParquetFileReader, error)
}

type ParquetFileWriter interface {
	io.Writer
	io.Closer
	Create(name string) (ParquetFileWriter, error)
}

const bufferSize = 4096

// Convert a file reater to Thrift reader
func ConvertToThriftReader(file ParquetFileReader, offset int64) *thrift.TBufferedTransport {
	if _, err := file.Seek(offset, 0); err != nil {
		return nil
	}
	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, bufferSize)
	return bufferReader
}
