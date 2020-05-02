package layout

import (
	"context"
	"reflect"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/compress"
	"github.com/xitongsys/parquet-go/encoding"
	"github.com/xitongsys/parquet-go/parquet"
)

type DictRecType struct {
	DictMap   map[interface{}]int32
	DictSlice []interface{}
	Type      parquet.Type
}

func NewDictRec(pT parquet.Type) *DictRecType {
	res := new(DictRecType)
	res.DictMap = make(map[interface{}]int32)
	res.Type = pT
	return res
}

func DictRecToDictPage(dictRec *DictRecType, pageSize int32, compressType parquet.CompressionCodec) (*Page, int64) {
	var totSize int64 = 0

	page := NewDataPage()
	page.PageSize = pageSize
	page.Header.DataPageHeader.NumValues = int32(len(dictRec.DictSlice))
	page.Header.Type = parquet.PageType_DICTIONARY_PAGE

	page.DataTable = new(Table)
	page.DataTable.Values = dictRec.DictSlice
	dataType := parquet.Type_INT32
	page.Schema = &parquet.SchemaElement{
		Type: &dataType,
	}
	page.CompressType = compressType

	page.DictPageCompress(compressType, dictRec.Type)
	totSize += int64(len(page.RawData))
	return page, totSize
}

//Compress the dict page to parquet file
func (page *Page) DictPageCompress(compressType parquet.CompressionCodec, pT parquet.Type) []byte {
	dataBuf := encoding.WritePlain(page.DataTable.Values, pT)
	var dataEncodeBuf []byte = compress.Compress(dataBuf, compressType)

	//pageHeader/////////////////////////////////////
	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DICTIONARY_PAGE
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	page.Header.UncompressedPageSize = int32(len(dataBuf))
	page.Header.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	page.Header.DictionaryPageHeader.NumValues = int32(len(page.DataTable.Values))
	page.Header.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(context.TODO(), page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res
	return res
}

//Convert a table to dict data pages
func TableToDictDataPages(dictRec *DictRecType, table *Table, pageSize int32, bitWidth int32, compressType parquet.CompressionCodec) ([]*Page, int64) {
	var totSize int64 = 0
	totalLn := len(table.Values)
	res := make([]*Page, 0)
	i := 0

	pT, cT := table.Schema.Type, table.Schema.ConvertedType

	for i < totalLn {
		j := i
		var size int32 = 0
		var numValues int32 = 0

		var maxVal interface{} = table.Values[i]
		var minVal interface{} = table.Values[i]
		values := make([]interface{}, 0)

		for j < totalLn && size < pageSize {
			if table.DefinitionLevels[j] == table.MaxDefinitionLevel {
				numValues++
				size += int32(common.SizeOf(reflect.ValueOf(table.Values[j])))
				maxVal = common.Max(maxVal, table.Values[j], pT, cT)
				minVal = common.Min(minVal, table.Values[j], pT, cT)
				if _, ok := dictRec.DictMap[table.Values[j]]; !ok {
					dictRec.DictSlice = append(dictRec.DictSlice, table.Values[j])
					dictRec.DictMap[table.Values[j]] = int32(len(dictRec.DictSlice) - 1)
				}
				values = append(values, int32(dictRec.DictMap[table.Values[j]]))
			}
			j++
		}

		page := NewDataPage()
		page.PageSize = pageSize
		page.Header.DataPageHeader.NumValues = numValues
		page.Header.Type = parquet.PageType_DATA_PAGE

		page.DataTable = new(Table)
		page.DataTable.RepetitionType = table.RepetitionType
		page.DataTable.Path = table.Path
		page.DataTable.MaxDefinitionLevel = table.MaxDefinitionLevel
		page.DataTable.MaxRepetitionLevel = table.MaxRepetitionLevel
		page.DataTable.Values = values
		page.DataTable.DefinitionLevels = table.DefinitionLevels[i:j]
		page.DataTable.RepetitionLevels = table.RepetitionLevels[i:j]
		page.MaxVal = maxVal
		page.MinVal = minVal
		page.Schema = table.Schema
		page.CompressType = compressType
		page.Path = table.Path
		page.Info = table.Info

		page.DictDataPageCompress(compressType, bitWidth)

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = j
	}
	return res, totSize
}

//Compress the data page to parquet file
func (page *Page) DictDataPageCompress(compressType parquet.CompressionCodec, bitWidth int32) []byte {
	ln := len(page.DataTable.DefinitionLevels)
	//values////////////////////////////////////////////
	valuesRawBuf := []byte{byte(bitWidth)}
	valuesRawBuf = append(valuesRawBuf, encoding.WriteRLE(page.DataTable.Values, bitWidth, parquet.Type_INT32)...)

	//definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	if page.DataTable.MaxDefinitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = int64(page.DataTable.DefinitionLevels[i])
		}
		definitionLevelBuf = encoding.WriteRLEBitPackedHybrid(numInterfaces,
			int32(common.BitNum(uint64(page.DataTable.MaxDefinitionLevel))),
			parquet.Type_INT64)
	}

	//repetitionLevel/////////////////////////////////
	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = int64(page.DataTable.RepetitionLevels[i])
		}
		repetitionLevelBuf = encoding.WriteRLEBitPackedHybrid(numInterfaces,
			int32(common.BitNum(uint64(page.DataTable.MaxRepetitionLevel))),
			parquet.Type_INT64)
	}

	//dataBuf = repetitionBuf + definitionBuf + valuesRawBuf
	var dataBuf []byte
	dataBuf = append(dataBuf, repetitionLevelBuf...)
	dataBuf = append(dataBuf, definitionLevelBuf...)
	dataBuf = append(dataBuf, valuesRawBuf...)

	var dataEncodeBuf []byte = compress.Compress(dataBuf, compressType)

	//pageHeader/////////////////////////////////////
	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DATA_PAGE
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	page.Header.UncompressedPageSize = int32(len(dataBuf))
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	page.Header.DataPageHeader.NumValues = int32(len(page.DataTable.DefinitionLevels))
	page.Header.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.Encoding = parquet.Encoding_PLAIN_DICTIONARY

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(context.TODO(), page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res

	return res
}

//Convert a table to dict page
func TableToDictPage(table *Table, pageSize int32, compressType parquet.CompressionCodec) (*Page, int64) {
	var totSize int64 = 0
	totalLn := len(table.Values)

	page := NewDataPage()
	page.PageSize = pageSize
	page.Header.DataPageHeader.NumValues = int32(totalLn)
	page.Header.Type = parquet.PageType_DICTIONARY_PAGE

	page.DataTable = new(Table)
	page.DataTable.RepetitionType = table.RepetitionType
	page.DataTable.Path = table.Path
	page.DataTable.MaxDefinitionLevel = table.MaxDefinitionLevel
	page.DataTable.MaxRepetitionLevel = table.MaxRepetitionLevel
	page.DataTable.Values = table.Values
	page.DataTable.DefinitionLevels = table.DefinitionLevels
	page.DataTable.RepetitionLevels = table.RepetitionLevels
	page.Schema = table.Schema
	page.CompressType = compressType
	page.Path = table.Path
	page.Info = table.Info

	page.DictPageCompress(compressType, *page.Schema.Type)
	totSize += int64(len(page.RawData))
	return page, totSize
}
