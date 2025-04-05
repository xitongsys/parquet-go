package layout

import (
	"context"
	"math/bits"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/common"
	"github.com/hangxie/parquet-go/compress"
	"github.com/hangxie/parquet-go/encoding"
	"github.com/hangxie/parquet-go/parquet"
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

func DictRecToDictPage(dictRec *DictRecType, pageSize int32, compressType parquet.CompressionCodec) (*Page, int64, error) {
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

	if _, err := page.DictPageCompress(compressType, dictRec.Type); err != nil {
		return nil, 0, err
	}
	totSize += int64(len(page.RawData))
	return page, totSize, nil
}

// Compress the dict page to parquet file
func (page *Page) DictPageCompress(compressType parquet.CompressionCodec, pT parquet.Type) ([]byte, error) {
	dataBuf, err := encoding.WritePlain(page.DataTable.Values, pT)
	if err != nil {
		return nil, err
	}
	var dataEncodeBuf []byte = compress.Compress(dataBuf, compressType)

	// pageHeader/////////////////////////////////////
	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DICTIONARY_PAGE
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	page.Header.UncompressedPageSize = int32(len(dataBuf))
	page.Header.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	page.Header.DictionaryPageHeader.NumValues = int32(len(page.DataTable.Values))
	page.Header.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(context.TODO(), page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res
	return res, nil
}

// Convert a table to dict data pages
func TableToDictDataPages(dictRec *DictRecType, table *Table, pageSize, bitWidth int32, compressType parquet.CompressionCodec) ([]*Page, int64, error) {
	var totSize int64 = 0
	totalLn := len(table.Values)
	res := make([]*Page, 0)
	i := 0

	pT, cT, logT, omitStats := table.Schema.Type, table.Schema.ConvertedType, table.Schema.LogicalType, table.Info.OmitStats

	for i < totalLn {
		j := i
		var size int32 = 0
		var numValues int32 = 0

		var maxVal interface{} = table.Values[i]
		var minVal interface{} = table.Values[i]
		var nullCount int64 = 0
		values := make([]int32, 0)

		funcTable := common.FindFuncTable(pT, cT, logT)

		for j < totalLn && size < pageSize {
			if table.DefinitionLevels[j] == table.MaxDefinitionLevel {
				numValues++
				var elSize int32
				if omitStats {
					_, _, elSize = funcTable.MinMaxSize(nil, nil, table.Values[j])
				} else {
					minVal, maxVal, elSize = funcTable.MinMaxSize(minVal, maxVal, table.Values[j])
				}
				size += elSize
				if idx, ok := dictRec.DictMap[table.Values[j]]; ok {
					values = append(values, idx)
				} else {
					dictRec.DictSlice = append(dictRec.DictSlice, table.Values[j])
					idx := int32(len(dictRec.DictSlice) - 1)
					dictRec.DictMap[table.Values[j]] = idx
					values = append(values, idx)
				}
			}
			if table.Values[i] == nil {
				nullCount++
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
		page.DataTable.DefinitionLevels = table.DefinitionLevels[i:j]
		page.DataTable.RepetitionLevels = table.RepetitionLevels[i:j]

		// Values in DataTable of a DictPage is nil for optimization.
		// page.DataTable.Values = values

		if !omitStats {
			page.MaxVal = maxVal
			page.MinVal = minVal
			page.NullCount = &nullCount
		}
		page.Schema = table.Schema
		page.CompressType = compressType
		page.Path = table.Path
		page.Info = table.Info

		_, err := page.DictDataPageCompress(compressType, bitWidth, values)
		if err != nil {
			return nil, 0, err
		}

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = j
	}
	return res, totSize, nil
}

// Compress the data page to parquet file
func (page *Page) DictDataPageCompress(compressType parquet.CompressionCodec, bitWidth int32, values []int32) ([]byte, error) {
	// values////////////////////////////////////////////
	valuesRawBuf := []byte{byte(bitWidth)}
	valuesRawBuf = append(valuesRawBuf, encoding.WriteRLEInt32(values, bitWidth)...)

	// definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	var err error
	if page.DataTable.MaxDefinitionLevel > 0 {
		definitionLevelBuf, err = encoding.WriteRLEBitPackedHybridInt32(
			page.DataTable.DefinitionLevels,
			int32(bits.Len32(uint32(page.DataTable.MaxDefinitionLevel))))
		if err != nil {
			return nil, err
		}
	}

	// repetitionLevel/////////////////////////////////
	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		repetitionLevelBuf, err = encoding.WriteRLEBitPackedHybridInt32(
			page.DataTable.RepetitionLevels,
			int32(bits.Len32(uint32(page.DataTable.MaxRepetitionLevel))))
		if err != nil {
			return nil, err
		}
	}

	// dataBuf = repetitionBuf + definitionBuf + valuesRawBuf
	var dataBuf []byte
	dataBuf = append(dataBuf, repetitionLevelBuf...)
	dataBuf = append(dataBuf, definitionLevelBuf...)
	dataBuf = append(dataBuf, valuesRawBuf...)

	var dataEncodeBuf []byte = compress.Compress(dataBuf, compressType)

	// pageHeader/////////////////////////////////////
	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DATA_PAGE
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	page.Header.UncompressedPageSize = int32(len(dataBuf))
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	page.Header.DataPageHeader.NumValues = int32(len(page.DataTable.DefinitionLevels))
	page.Header.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.Encoding = parquet.Encoding_PLAIN_DICTIONARY

	page.Header.DataPageHeader.Statistics = parquet.NewStatistics()
	if page.MaxVal != nil {
		tmpBuf, err := encoding.WritePlain([]interface{}{page.MaxVal}, *page.Schema.Type)
		if err != nil {
			return nil, err
		}
		if *page.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Max = tmpBuf
		page.Header.DataPageHeader.Statistics.MaxValue = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf, err := encoding.WritePlain([]interface{}{page.MinVal}, *page.Schema.Type)
		if err != nil {
			return nil, err
		}
		if *page.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Min = tmpBuf
		page.Header.DataPageHeader.Statistics.MinValue = tmpBuf
	}

	page.Header.DataPageHeader.Statistics.NullCount = page.NullCount

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(context.TODO(), page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res

	return res, nil
}
