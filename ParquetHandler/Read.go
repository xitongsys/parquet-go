package ParquetHandler

import (
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/Marshal"
	. "github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
)

func ConvertToThriftReader(file ParquetFile, offset int64, size int64) *thrift.TBufferedTransport {
	file.Seek(int(offset), 0)

	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(size))
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
	for _, schema := range self.Footer.Schema {
		schema.Name = HeadToUpper(schema.Name)
	}

	for _, rowGroup := range self.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			path := chunk.MetaData.PathInSchema
			ln := len(path)
			for i := 0; i < ln; i++ {
				path[i] = HeadToUpper(path[i])
			}
		}
	}

}

func (self *ParquetHandler) ReadInit(pfile ParquetFile, np int64) int {
	self.PFile = pfile
	self.NP = np
	self.ReadFooter()
	self.SchemaHandler = NewSchemaHandlerFromSchemaList(self.Footer.GetSchema())
	self.RowGroupIndex = 0
	return len(self.Footer.GetRowGroups())
}

func (self *ParquetHandler) ReadOneRowGroup() (*map[string]*Table, int) {
	rowGroups := self.Footer.GetRowGroups()
	ln := int64(len(rowGroups))
	if self.RowGroupIndex >= ln {
		return nil, 0
	}

	rowGroupHeader := self.Footer.GetRowGroups()[self.RowGroupIndex]
	self.RowGroupIndex++
	rowGroup := self.ReadRowGroup(rowGroupHeader)
	return rowGroup.RowGroupToTableMap(), int(rowGroup.RowGroupHeader.GetNumRows())
}

func (self *ParquetHandler) ReadOneRowGroupAndUnmarshal(dstInterface interface{}) {
	tmap, num := self.ReadOneRowGroup()
	ot := reflect.TypeOf(dstInterface).Elem().Elem()
	dstList := make([]interface{}, self.NP)
	delta := (int64(num) + self.NP - 1) / self.NP

	doneChan := make(chan int)
	for c := int64(0); c < self.NP; c++ {
		bgn := c * delta
		end := bgn + delta
		if end > int64(num) {
			end = int64(num)
		}
		if bgn >= int64(num) {
			bgn, end = int64(num), int64(num)
		}
		go func(b, e, index int) {
			dstList[index] = reflect.New(reflect.SliceOf(ot)).Interface()
			Unmarshal(tmap, b, e, dstList[index], self.SchemaHandler)
			doneChan <- 0
		}(int(bgn), int(end), int(c))
	}
	for c := int64(0); c < self.NP; c++ {
		<-doneChan
	}

	resTmp := reflect.MakeSlice(reflect.SliceOf(ot), 0, num)
	for _, dst := range dstList {
		resTmp = reflect.AppendSlice(resTmp, reflect.ValueOf(dst).Elem())
	}

	reflect.ValueOf(dstInterface).Elem().Set(resTmp)

}
