package ParquetFile

import (
	"git.apache.org/thrift.git/lib/go/thrift"
)

type ParquetFile interface {
	Seek(offset int, pos int) (int64, error)
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close()
	Open(name string) (ParquetFile, error)
	Create(name string) (ParquetFile, error)
}

//Convert a file reater to Thrift reader
func ConvertToThriftReader(file ParquetFile, offset int64, size int64) *thrift.TBufferedTransport {
	file.Seek(int(offset), 0)
	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(size))
	return bufferReader
}
