package Reader

import (
	. "Layout"
	. "SchemaHandler"
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"os"
	"parquet"
)

func ConvertToThriftReader(file *os.File, offset int64) *thrift.TBufferedTransport {
	file.Seek(offset, 0)
	num, _ := file.Seek(0, 2)
	file.Seek(offset, 0)

	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(num))
	return bufferReader
}

func GetFooterSize(file *os.File) uint32 {
	buf := make([]byte, 4)
	file.Seek(-8, 2)
	file.Read(buf)
	size := binary.LittleEndian.Uint32(buf)
	log.Println("Foot Size = ", size)
	return size
}

func GetFooter(file *os.File, size uint32) *parquet.FileMetaData {
	file.Seek(-(int64)(8+size), 2)
	footer := parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactory()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(file))
	footer.Read(protocol)
	return footer
}

func Reader(file *os.File) []*RowGroup {
	rowGroups := make([]*RowGroup, 0)

	footer := GetFooter(file, GetFooterSize(file))
	log.Println(footer)

	schemaHandler := NewSchemaHandlerFromSchemaList(footer.GetSchema())

	for _, rowGroupHeader := range footer.GetRowGroups() {
		rowGroup := ReadRowGroup(file, schemaHandler, rowGroupHeader)
		rowGroups = append(rowGroups, rowGroup)
	}
	return rowGroups
}
