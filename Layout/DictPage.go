package Layout

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Compress"
	"github.com/xitongsys/parquet-go/ParquetEncoding"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
)

//Convert a table to data pages
func TableToDictDataPages(table *Table, pageSize int32, bitWidth int32, compressType parquet.CompressionCodec) ([]*Page, int64) {
	var totSize int64 = 0
	totalLn := len(table.Values)
	res := make([]*Page, 0)
	i := 0
	dataType := table.Type

	for i < totalLn {
		j := i + 1
		var size int32 = 0
		var numValues int32 = 0

		for j < totalLn && size < pageSize {
			if table.DefinitionLevels[j] == table.MaxDefinitionLevel {
				numValues++
				size += int32(Common.SizeOf(reflect.ValueOf(table.Values[j])))
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
		page.DataTable.Values = table.Values[i:j]
		page.DataTable.DefinitionLevels = table.DefinitionLevels[i:j]
		page.DataTable.RepetitionLevels = table.RepetitionLevels[i:j]
		page.DataType = dataType
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
	valuesBuf := make([]interface{}, 0)
	for i := 0; i < ln; i++ {
		if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
			valuesBuf = append(valuesBuf, page.DataTable.Values[i])
		}
	}
	valuesRawBuf := []byte{byte(bitWidth)}
	valuesRawBuf = append(valuesRawBuf, ParquetEncoding.WriteRLE(valuesBuf, bitWidth)...)

	//definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	if page.DataTable.MaxDefinitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = ParquetType.INT64(page.DataTable.DefinitionLevels[i])
		}
		definitionLevelBuf = ParquetEncoding.WriteRLEBitPackedHybrid(numInterfaces, int32(Common.BitNum(uint64(page.DataTable.MaxDefinitionLevel))))
	}

	//repetitionLevel/////////////////////////////////
	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = ParquetType.INT64(page.DataTable.RepetitionLevels[i])
		}
		repetitionLevelBuf = ParquetEncoding.WriteRLEBitPackedHybrid(numInterfaces, int32(Common.BitNum(uint64(page.DataTable.MaxRepetitionLevel))))
	}

	//dataBuf = repetitionBuf + definitionBuf + valuesRawBuf
	var dataBuf []byte
	dataBuf = append(dataBuf, repetitionLevelBuf...)
	dataBuf = append(dataBuf, definitionLevelBuf...)
	dataBuf = append(dataBuf, valuesRawBuf...)

	var dataEncodeBuf []byte
	if compressType == parquet.CompressionCodec_GZIP {
		dataEncodeBuf = Compress.CompressGzip(dataBuf)
	} else if compressType == parquet.CompressionCodec_SNAPPY {
		dataEncodeBuf = Compress.CompressSnappy(dataBuf)
	} else {
		dataEncodeBuf = dataBuf
	}

	//pageHeader/////////////////////////////////////
	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DATA_PAGE
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	page.Header.UncompressedPageSize = int32(len(dataBuf))
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	page.Header.DataPageHeader.NumValues = int32(len(page.DataTable.Values))
	page.Header.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.Encoding = parquet.Encoding_RLE

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(page.Header)

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
	dataType := table.Type

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
	page.DataType = dataType
	page.CompressType = compressType
	page.Path = table.Path
	page.Info = table.Info

	page.DictPageCompress(compressType)
	totSize += int64(len(page.RawData))
	return page, totSize
}

//Compress the dict page to parquet file
func (page *Page) DictPageCompress(compressType parquet.CompressionCodec) []byte {
	dataBuf := ParquetEncoding.WritePlain(page.DataTable.Values)
	var dataEncodeBuf []byte
	if compressType == parquet.CompressionCodec_GZIP {
		dataEncodeBuf = Compress.CompressGzip(dataBuf)
	} else if compressType == parquet.CompressionCodec_SNAPPY {
		dataEncodeBuf = Compress.CompressSnappy(dataBuf)
	} else {
		dataEncodeBuf = dataBuf
	}

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
	pageHeaderBuf, _ := ts.Write(page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res
	return res
}
