package source

import (
	"io"

	"github.com/apache/thrift/lib/go/thrift"
)

type ParquetFileR interface {
	io.Seeker
	io.Reader
	io.Closer
	Open(name string) (ParquetFileR, error)
}

type ParquetFileW interface {
	io.Writer
	io.Closer
}

type ParquetFile interface {
	ParquetFileR
	ParquetFileW
	Create(name string) (ParquetFile, error)
}

const bufferSize = 4096

//Convert a file reater to Thrift reader
func ConvertToThriftReader(file ParquetFileR, offset int64) *thrift.TBufferedTransport {
	file.Seek(offset, 0)
	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, bufferSize)
	return bufferReader
}
