package ParquetHandler

import (
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

func ConvertToThriftReader(file ParquetFile, offset int64) *thrift.TBufferedTransport {
	file.Seek(int(offset), 0)
	num, _ := file.Seek(0, 2)
	file.Seek(int(offset), 0)

	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(num))
	return bufferReader
}

func (self *ParquetHandler) GetFooterSize() uint32 {
	buf := make([]byte, 4)
	self.PFile.Seek(-8, 2)
	self.PFile.Read(buf)
	size := binary.LittleEndian.Uint32(buf)
	return size
}

func (self *ParquetHandler) ReadFooter() {
	size := self.GetFooterSize()
	self.PFile.Seek(int(-(int64)(8+size)), 2)
	self.Footer = parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactory()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(self.PFile))
	self.Footer.Read(protocol)
}

func (self *ParquetHandler) ReadInit(pfile ParquetFile, np int64) int {
	self.PFile = pfile
	self.NP = np
	self.ReadFooter()
	self.SchemaHandler = NewSchemaHandlerFromSchemaList(self.Footer.GetSchema())
	self.RowGroupIndex = 0
	return len(self.Footer.GetRowGroups())
}

func (self *ParquetHandler) ReadOneRowGroup() *map[string]*Table {
	rowGroups := self.Footer.GetRowGroups()
	ln := int64(len(rowGroups))
	if self.RowGroupIndex >= ln {
		return nil
	}

	rowGroupHeader := self.Footer.GetRowGroups()[self.RowGroupIndex]
	self.RowGroupIndex++
	rowGroup := self.ReadRowGroup(rowGroupHeader)
	return rowGroup.RowGroupToTableMap()
}
