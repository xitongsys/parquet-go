package Layout

import (
	. "Common"
	. "Compress"
	. "PEncoding"
	. "ParquetType"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"parquet"
	"reflect"
)

//functions in this file: assumed the page is dataPage; dictPage will add soon

type Page struct {
	Header       *parquet.PageHeader
	DataTable    *Table
	RawData      []byte
	CompressType parquet.CompressionCodec
	DataType     parquet.Type
	MaxVal       interface{}
	MinVal       interface{}
}

func NewPage() *Page {
	page := new(Page)
	page.DataTable = nil
	page.Header = parquet.NewPageHeader()
	return page
}

func NewDictPage() *Page {
	page := NewPage()
	page.Header.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	return page
}

func NewDataPage() *Page {
	page := NewPage()
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	return page
}

func TableToDataPages(table *Table, pageSize int32, compressType parquet.CompressionCodec) ([]*Page, int64) {
	var totSize int64 = 0
	totalLn := len(table.Values)
	res := make([]*Page, 0)
	i := 0
	dataType := table.Type

	for i < totalLn {
		j := i + 1
		var size int32 = 0
		var numValues int32 = 0

		var maxVal interface{} = table.Values[i]
		var minVal interface{} = table.Values[i]

		for j < totalLn && size < pageSize {
			if table.DefinitionLevels[j] == table.MaxDefinitionLevel {
				numValues++
				size += int32(SizeOf(reflect.ValueOf(table.Values[j])))
				maxVal = Max(maxVal, table.Values[j])
				minVal = Min(minVal, table.Values[j])
			}
			j++
		}

		page := NewDataPage()
		page.Header.DataPageHeader.NumValues = numValues
		page.Header.Type = parquet.PageType_DATA_PAGE

		page.DataTable = new(Table)
		page.DataTable.Repetition_Type = table.Repetition_Type
		page.DataTable.Path = table.Path
		page.DataTable.MaxDefinitionLevel = table.MaxDefinitionLevel
		page.DataTable.MaxRepetitionLevel = table.MaxRepetitionLevel
		page.DataTable.Values = table.Values[i:j]
		page.DataTable.DefinitionLevels = table.DefinitionLevels[i:j]
		page.DataTable.RepetitionLevels = table.RepetitionLevels[i:j]
		page.MaxVal = maxVal
		page.MinVal = minVal
		page.DataType = dataType
		page.CompressType = compressType

		page.DataPageCompress(compressType)

		totSize += int64(len(page.RawData))
		res = append(res, page)

		i = j
	}

	log.Println("TableToPages Finished")
	return res, totSize
}

func (page *Page) DataPageCompress(compressType parquet.CompressionCodec) []byte {
	ln := len(page.DataTable.DefinitionLevels)

	//values////////////////////////////////////////////
	valuesBuf := make([]interface{}, 0)
	for i := 0; i < ln; i++ {
		if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
			valuesBuf = append(valuesBuf, page.DataTable.Values[i])
		}
	}
	valuesRawBuf := WritePlain(valuesBuf)

	////////test DeltaINT64///////////////////
	if page.DataType == parquet.Type_INT64 {
		//valuesRawBuf = WriteDeltaINT64(valuesBuf)
		//log.Println(valuesRawBuf)
	}
	if page.DataType == parquet.Type_INT32 {
		valuesRawBuf = WriteDeltaINT32(valuesBuf)
		//log.Println(valuesRawBuf)
	}
	if page.DataType == parquet.Type_BYTE_ARRAY {
		valuesRawBuf = WriteDeltaLengthByteArray(valuesBuf)
	}
	////////////////////////////////////////////

	//definitionLevel//////////////////////////////////
	definitionLevelBuf := make([]byte, 0)
	if page.DataTable.MaxDefinitionLevel > 0 {
		i := 0
		rleBuf := make([]byte, 0)
		for i < ln {
			j := i + 1
			for j < ln && page.DataTable.DefinitionLevels[j] == page.DataTable.DefinitionLevels[i] {
				j++
			}
			num := j - i
			rleBufCur := WriteRLE(uint64(page.DataTable.DefinitionLevels[i]), int32(num),
				int32(BitNum(uint64(page.DataTable.MaxDefinitionLevel))))

			rleBuf = append(rleBuf, rleBufCur...)

			i = j
		}

		tmpBuf := make([]interface{}, 1)
		tmpBuf[0] = INT32(len(rleBuf))
		lengthBuf := WritePlainINT32(tmpBuf)

		definitionLevelBuf = append(definitionLevelBuf, lengthBuf...)
		definitionLevelBuf = append(definitionLevelBuf, rleBuf...)
	}

	//repetitionLevel/////////////////////////////////
	repetitionLevelBuf := make([]byte, 0)
	if page.DataTable.MaxRepetitionLevel > 0 {
		i := 0
		rleBuf := make([]byte, 0)
		for i < ln {
			j := i + 1
			for j < ln && page.DataTable.RepetitionLevels[j] == page.DataTable.RepetitionLevels[i] {
				j++
			}

			num := j - i
			rleBufCur := WriteRLE(uint64(page.DataTable.RepetitionLevels[i]), int32(num),
				int32(BitNum(uint64(page.DataTable.MaxRepetitionLevel))))

			rleBuf = append(rleBuf, rleBufCur...)

			i = j
		}
		tmpBuf := make([]interface{}, 1)
		tmpBuf[0] = INT32(len(rleBuf))
		lengthBuf := WritePlainINT32(tmpBuf)

		repetitionLevelBuf = append(repetitionLevelBuf, lengthBuf...)
		repetitionLevelBuf = append(repetitionLevelBuf, rleBuf...)

	}

	//dataBuf = repetitionBuf + definitionBuf + valuesRawBuf
	dataBuf := make([]byte, 0)
	dataBuf = append(dataBuf, repetitionLevelBuf...)
	dataBuf = append(dataBuf, definitionLevelBuf...)
	dataBuf = append(dataBuf, valuesRawBuf...)
	dataEncodeBuf := make([]byte, 0)
	if compressType == parquet.CompressionCodec_GZIP {
		dataEncodeBuf = CompressGzip(dataBuf)
	} else if compressType == parquet.CompressionCodec_SNAPPY {
		dataEncodeBuf = CompressSnappy(dataBuf)
	} else {
		dataEncodeBuf = dataBuf
	}

	//pageHeader/////////////////////////////////////
	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DATA_PAGE
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	page.Header.UncompressedPageSize = int32(len(dataBuf))
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	page.Header.DataPageHeader.NumValues = int32(len(valuesBuf))
	page.Header.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.Encoding = parquet.Encoding_PLAIN

	/////////test DeltaINT64////////////////
	if page.DataType == parquet.Type_INT64 {
		//page.Header.DataPageHeader.Encoding = parquet.Encoding_DELTA_BINARY_PACKED
	}
	if page.DataType == parquet.Type_INT32 {
		page.Header.DataPageHeader.Encoding = parquet.Encoding_DELTA_BINARY_PACKED
	}
	if page.DataType == parquet.Type_BYTE_ARRAY {
		page.Header.DataPageHeader.Encoding = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
	}
	//////////////////////////////////////

	page.Header.DataPageHeader.Statistics = parquet.NewStatistics()
	page.Header.DataPageHeader.Statistics.Max = WritePlain([]interface{}{page.MaxVal})
	page.Header.DataPageHeader.Statistics.Min = WritePlain([]interface{}{page.MinVal})

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(page.Header)

	res := make([]byte, 0)
	res = append(res, pageHeaderBuf...)
	res = append(res, dataEncodeBuf...)

	page.RawData = res

	return res
}

//ToDo
func TableToDictDataPages(table *Table, pageSize int32, compressType parquet.CompressionCodec) ([]*Page, int64) {
	return []*Page{}, 0
}

//ToDo
func (page *Page) DictPageCompress(compressType parquet.CompressionCodec) []byte {
	return []byte{}
}
