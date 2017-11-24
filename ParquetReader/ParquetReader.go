package ParquetReader

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
)

//Convert a file reater to Thrift reader
func ConvertToThriftReader(file ParquetFile, offset int64, size int64) *thrift.TBufferedTransport {
	file.Seek(int(offset), 0)
	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(size))
	return bufferReader
}

type ParquetReader struct {
	SchemaHandler *SchemaHandler.SchemaHandler
	NP            int64 //parallel number
	Footer        *parquet.FileMetaData
	PFile         ParquetFile

	ColumnBuffers map[string]*ColumnBufferType
}

//Create a parquet reader
func NewParquetReader() *ParquetReader {
	res := new(ParquetReader)
	res.NP = 1
	return res
}

//Get the footer size
func (self *ParquetReader) GetFooterSize() uint32 {
	buf := make([]byte, 4)
	self.PFile.Seek(-8, 2)
	self.PFile.Read(buf)
	size := binary.LittleEndian.Uint32(buf)
	return size
}

//Read footer from parquet file
func (self *ParquetReader) ReadFooter() {
	size := self.GetFooterSize()
	self.PFile.Seek(int(-(int64)(8+size)), 2)
	self.Footer = parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactory()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(self.PFile))
	self.Footer.Read(protocol)
}

// init function. np is the parallel number
func (self *ParquetReader) ReadInit(pfile ParquetFile, np int64) {
	self.PFile = pfile
	self.NP = np
	self.ReadFooter()
	self.SchemaHandler = SchemaHandler.NewSchemaHandlerFromSchemaList(self.Footer.GetSchema())
	self.RowGroupIndex = 0
	return len(self.Footer.GetRowGroups())
}

func (self *ParquetReader) Read(interface{}) {

}
