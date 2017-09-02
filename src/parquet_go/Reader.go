package parquet_go

import (
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"os"
	"parquet"
	"strings"
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

func GetPageHeader(thriftReader *thrift.TBufferedTransport) *parquet.PageHeader {
	protocol := thrift.NewTCompactProtocol(thriftReader)
	pageHeader := parquet.NewPageHeader()
	pageHeader.Read(protocol)
	return pageHeader
}

func ReadChunk(file *os.File, schemaHandler *SchemaHandler, colMetaData *parquet.ColumnMetaData, numRows int64) map[string]*Table {
	chunkTableMap := make(map[string]*Table)
	//get page offset
	dataPageOffset := colMetaData.GetDataPageOffset()
	//dictionaryPageOffset := colMetaData.GetDictionaryPageOffset()

	var dictData []Interface

	thriftReader := ConvertToThriftReader(file, dataPageOffset)

	var valueCnt int64 = 0
	for valueCnt < numRows {
		pageHeader := GetPageHeader(thriftReader)
		pageType := pageHeader.GetType()

		log.Println(pageHeader)

		if pageType == parquet.PageType_DATA_PAGE {
			tableCur := ReadDataPage(thriftReader, schemaHandler, colMetaData, pageHeader, dictData)
			pathStr := strings.Join(tableCur.Path, ".")

			if _, ok := chunkTableMap[pathStr]; ok {
				MergeTable(chunkTableMap[pathStr], tableCur)
			} else {
				chunkTableMap[pathStr] = tableCur
			}
			valueCnt += int64(pageHeader.GetDataPageHeader().GetNumValues())

		} else if pageType == parquet.PageType_DICTIONARY_PAGE {
			dictData = ReadDictPage(thriftReader, schemaHandler, colMetaData, pageHeader)
		} else {
			log.Println("Skipping unknown page type =", pageType)
		}
	}

	return chunkTableMap
}

func Reader(file *os.File) map[string]*Table {
	tableMap := make(map[string]*Table)
	footer := GetFooter(file, GetFooterSize(file))
	
	log.Println(footer)
	
	schemaHandler := NewSchemaHandlerFromSchema(footer.GetSchema())

	for _, rowGroup := range footer.GetRowGroups() {
		numRows := rowGroup.GetNumRows()
		log.Println("RowGroup.num_rows=", numRows, "RowGroup.ColumnChunkNum=", len(rowGroup.GetColumns()))

		for _, columnChunk := range rowGroup.GetColumns() {
			colMetaData := columnChunk.GetMetaData()
			chunkTableMap := ReadChunk(file, schemaHandler, colMetaData, colMetaData.NumValues)
			for key, value := range chunkTableMap {
				if _, ok := tableMap[key]; ok {
					MergeTable(tableMap[key], value)
				} else {
					tableMap[key] = value
				}
			}
		}
	}
	return tableMap
}
