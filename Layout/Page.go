package Layout

import (
	"bytes"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Compress"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/ParquetEncoding"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"log"
	"reflect"
	"strings"
)

//Page is used to store the page data
type Page struct {
	//Header of a page
	Header *parquet.PageHeader
	//Table to store values
	DataTable *Common.Table
	//Compressed data of the page, which is written in parquet file
	RawData []byte
	//Compress type: gzip/snappy/none
	CompressType parquet.CompressionCodec
	//Parquet type of the values in the page
	DataType parquet.Type
	//Path in schema(include the root)
	Path []string
	//Maximum of the values
	MaxVal interface{}
	//Minimum of the values
	MinVal interface{}
}

//Create a new page
func NewPage() *Page {
	page := new(Page)
	page.DataTable = nil
	page.Header = parquet.NewPageHeader()
	return page
}

//Create a new dict page
func NewDictPage() *Page {
	page := NewPage()
	page.Header.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	return page
}

//Create a new data page
func NewDataPage() *Page {
	page := NewPage()
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	return page
}

//Convert a table to data pages
func TableToDataPages(table *Common.Table, pageSize int32, compressType parquet.CompressionCodec) ([]*Page, int64) {
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
				size += int32(Common.SizeOf(reflect.ValueOf(table.Values[j])))
				maxVal = Common.Max(maxVal, table.Values[j])
				minVal = Common.Min(minVal, table.Values[j])
			}
			j++
		}

		page := NewDataPage()
		page.Header.DataPageHeader.NumValues = numValues
		page.Header.Type = parquet.PageType_DATA_PAGE

		page.DataTable = new(Common.Table)
		page.DataTable.RepetitionType = table.RepetitionType
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
		page.Path = table.Path

		page.DataPageCompress(compressType)

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = j
	}
	return res, totSize
}

//
func (page *Page) Decode(dictPage *Page) {
	if dictPage == nil {
		return
	}
	numValues := len(page.DataTable.Values)
	for i := 0; i < numValues; i++ {
		if page.DataTable.Values[i] != nil {
			index := page.DataTable.Values[i].(ParquetType.INT64)
			page.DataTable.Values[i] = dictPage.DataTable.Values[index]
		}
	}
}

//Compress the data page to parquet file
func (page *Page) DataPageCompress(compressType parquet.CompressionCodec) []byte {
	ln := len(page.DataTable.DefinitionLevels)

	//values////////////////////////////////////////////
	valuesBuf := make([]interface{}, 0)
	for i := 0; i < ln; i++ {
		if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
			valuesBuf = append(valuesBuf, page.DataTable.Values[i])
		}
	}
	valuesRawBuf := ParquetEncoding.WritePlain(valuesBuf)

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
	page.Header.DataPageHeader.Encoding = parquet.Encoding_PLAIN

	/*
		/////////test DeltaINT64////////////////
		if page.DataType == parquet.Type_INT64 {
			//page.Header.DataPageHeader.Encoding = parquet.Encoding_DELTA_BINARY_PACKED
		}
		if page.DataType == parquet.Type_INT32 {
			page.Header.DataPageHeader.Encoding = parquet.Encoding_DELTA_BINARY_PACKED
		}
		if page.DataType == parquet.Type_BYTE_ARRAY {
			page.Header.DataPageHeader.Encoding = parquet.Encoding_DELTA_BYTE_ARRAY
		}
		//////////////////////////////////////
	*/
	page.Header.DataPageHeader.Statistics = parquet.NewStatistics()
	if page.MaxVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MaxVal})
		name := reflect.TypeOf(page.MaxVal).Name()
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Max = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MinVal})
		name := reflect.TypeOf(page.MinVal).Name()
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Min = tmpBuf
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res

	return res
}

//Compress data page v2 to parquet file
func (page *Page) DataPageV2Compress(compressType parquet.CompressionCodec) []byte {
	ln := len(page.DataTable.DefinitionLevels)

	//values////////////////////////////////////////////
	valuesBuf := make([]interface{}, 0)
	for i := 0; i < ln; i++ {
		if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
			valuesBuf = append(valuesBuf, page.DataTable.Values[i])
		}
	}
	valuesRawBuf := ParquetEncoding.WritePlain(valuesBuf)

	//definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	if page.DataTable.MaxDefinitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = ParquetType.INT64(page.DataTable.DefinitionLevels[i])
		}
		definitionLevelBuf = ParquetEncoding.WriteRLE(numInterfaces, int32(Common.BitNum(uint64(page.DataTable.MaxDefinitionLevel))))
	}

	//repetitionLevel/////////////////////////////////
	r0Num := int32(0)
	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = ParquetType.INT64(page.DataTable.RepetitionLevels[i])
			if page.DataTable.RepetitionLevels[i] == 0 {
				r0Num++
			}
		}
		repetitionLevelBuf = ParquetEncoding.WriteRLE(numInterfaces, int32(Common.BitNum(uint64(page.DataTable.MaxRepetitionLevel))))
	}

	var dataEncodeBuf []byte
	if compressType == parquet.CompressionCodec_GZIP {
		dataEncodeBuf = Compress.CompressGzip(valuesRawBuf)
	} else if compressType == parquet.CompressionCodec_SNAPPY {
		dataEncodeBuf = Compress.CompressSnappy(valuesRawBuf)
	} else {
		dataEncodeBuf = valuesRawBuf
	}

	//pageHeader/////////////////////////////////////
	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DATA_PAGE_V2
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf) + len(definitionLevelBuf) + len(repetitionLevelBuf))
	page.Header.UncompressedPageSize = int32(len(valuesRawBuf) + len(definitionLevelBuf) + len(repetitionLevelBuf))
	page.Header.DataPageHeaderV2 = parquet.NewDataPageHeaderV2()
	page.Header.DataPageHeaderV2.NumValues = int32(len(page.DataTable.Values))
	page.Header.DataPageHeaderV2.NumNulls = page.Header.DataPageHeaderV2.NumValues - int32(len(valuesBuf))
	page.Header.DataPageHeaderV2.NumRows = r0Num
	page.Header.DataPageHeaderV2.Encoding = parquet.Encoding_PLAIN
	page.Header.DataPageHeaderV2.DefinitionLevelsByteLength = int32(len(definitionLevelBuf))
	page.Header.DataPageHeaderV2.RepetitionLevelsByteLength = int32(len(repetitionLevelBuf))
	page.Header.DataPageHeaderV2.IsCompressed = true

	page.Header.DataPageHeaderV2.Statistics = parquet.NewStatistics()
	if page.MaxVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MaxVal})
		name := reflect.TypeOf(page.MaxVal).Name()
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeaderV2.Statistics.Max = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MinVal})
		name := reflect.TypeOf(page.MinVal).Name()
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeaderV2.Statistics.Min = tmpBuf
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, repetitionLevelBuf...)
	res = append(res, definitionLevelBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res

	return res
}

//Convert table to dict data page. ToDo
func TableToDictDataPages(table *Common.Table, pageSize int32, compressType parquet.CompressionCodec) ([]*Page, int64) {
	return []*Page{}, 0
}

//Compress dict page. ToDo
func (page *Page) DictPageCompress(compressType parquet.CompressionCodec) []byte {
	return []byte{}
}

//Read page header
func ReadPageHeader(thriftReader *thrift.TBufferedTransport) *parquet.PageHeader {
	protocol := thrift.NewTCompactProtocol(thriftReader)
	pageHeader := parquet.NewPageHeader()
	pageHeader.Read(protocol)
	return pageHeader
}

//Read data page values
func ReadDataPageValues(bytesReader *bytes.Reader, encoding parquet.Encoding, dataType parquet.Type, convertedType parquet.ConvertedType, cnt uint64, bitWidth uint64) []interface{} {
	if encoding == parquet.Encoding_PLAIN {
		return ParquetEncoding.ReadPlain(bytesReader, dataType, convertedType, cnt, bitWidth)

	} else if encoding == parquet.Encoding_PLAIN_DICTIONARY {
		b, _ := bytesReader.ReadByte()
		bitWidth = uint64(b)
		return ParquetEncoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, uint64(bytesReader.Len()))

	} else if encoding == parquet.Encoding_RLE {
		values := ParquetEncoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, 0)
		if dataType == parquet.Type_INT32 {
			for i := 0; i < len(values); i++ {
				values[i] = ParquetType.INT32(values[i].(ParquetType.INT64))
			}
		}
		return values

	} else if encoding == parquet.Encoding_BIT_PACKED {
		//deprecated
	} else if encoding == parquet.Encoding_DELTA_BINARY_PACKED {
		values := ParquetEncoding.ReadDeltaBinaryPackedINT(bytesReader)
		if dataType == parquet.Type_INT32 {
			for i := 0; i < len(values); i++ {
				values[i] = ParquetType.INT32(values[i].(ParquetType.INT64))
			}
		}
		return values

	} else if encoding == parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY {
		values := ParquetEncoding.ReadDeltaLengthByteArray(bytesReader)
		if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			for i := 0; i < len(values); i++ {
				values[i] = ParquetType.FIXED_LEN_BYTE_ARRAY(values[i].(ParquetType.BYTE_ARRAY))
			}
		}
		return values

	} else if encoding == parquet.Encoding_DELTA_BYTE_ARRAY {
		values := ParquetEncoding.ReadDeltaByteArray(bytesReader)
		if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			for i := 0; i < len(values); i++ {
				values[i] = ParquetType.FIXED_LEN_BYTE_ARRAY(values[i].(ParquetType.BYTE_ARRAY))
			}
		}
		return values

	} else if encoding == parquet.Encoding_RLE_DICTIONARY {
	} else {
		log.Println("Error Encoding method")
	}
	log.Println("Encoding Not Supported Yet")
	return make([]interface{}, 0)
}

//Read page from parquet file
func ReadPage(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler.SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Layout.Page, int64, int64) {
	pageHeader := ReadPageHeader(thriftReader)

	buf := make([]byte, 0)

	var page *Layout.Page
	compressedPageSize := pageHeader.GetCompressedPageSize()

	if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		dll := pageHeader.DataPageHeaderV2.GetDefinitionLevelsByteLength()
		rll := pageHeader.DataPageHeaderV2.GetRepetitionLevelsByteLength()
		repetitionLevelsBuf := make([]byte, rll)
		definitionLevelsBuf := make([]byte, dll)
		dataBuf := make([]byte, compressedPageSize-rll-dll)

		thriftReader.Read(repetitionLevelsBuf)
		thriftReader.Read(definitionLevelsBuf)
		thriftReader.Read(dataBuf)
		codec := colMetaData.GetCodec()
		if codec == parquet.CompressionCodec_GZIP {
			dataBuf = Compress.UncompressGzip(dataBuf)
		} else if codec == parquet.CompressionCodec_SNAPPY {
			dataBuf = Compress.UncompressSnappy(dataBuf)
		} else if codec == parquet.CompressionCodec_UNCOMPRESSED {
			dataBuf = dataBuf
		} else {
			log.Panicln("Unsupported Codec: ", codec)
		}
		tmpBuf := make([]byte, 0)
		if rll > 0 {
			tmpBuf = ParquetEncoding.WritePlainINT32([]interface{}{ParquetType.INT32(rll)})
			tmpBuf = append(tmpBuf, repetitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		if dll > 0 {
			tmpBuf = ParquetEncoding.WritePlainINT32([]interface{}{ParquetType.INT32(dll)})
			tmpBuf = append(tmpBuf, definitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		buf = append(buf, dataBuf...)

	} else {
		buf = make([]byte, compressedPageSize)
		thriftReader.Read(buf)
		codec := colMetaData.GetCodec()
		if codec == parquet.CompressionCodec_GZIP {
			buf = Compress.UncompressGzip(buf)
		} else if codec == parquet.CompressionCodec_SNAPPY {
			buf = Compress.UncompressSnappy(buf)
		} else if codec == parquet.CompressionCodec_UNCOMPRESSED {
			buf = buf
		} else {
			log.Panicln("Unsupported Codec: ", codec)
		}
	}

	bytesReader := bytes.NewReader(buf)
	path := make([]string, 0)
	path = append(path, schemaHandler.GetRootName())
	path = append(path, colMetaData.GetPathInSchema()...)
	name := strings.Join(path, ".")

	if pageHeader.GetType() == parquet.PageType_DATA_PAGE {
		page = Layout.NewDataPage()
		page.Header = pageHeader
		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)

		var repetitionLevels []interface{}
		if maxRepetitionLevel > 0 {
			bitWidth := Common.BitNum(uint64(maxRepetitionLevel))

			repetitionLevels = ReadDataPageValues(bytesReader,
				pageHeader.DataPageHeader.GetRepetitionLevelEncoding(),
				parquet.Type_INT64,
				-1,
				uint64(pageHeader.DataPageHeader.GetNumValues()),
				bitWidth)

		} else {
			repetitionLevels = make([]interface{}, pageHeader.DataPageHeader.GetNumValues())
			for i := 0; i < len(repetitionLevels); i++ {
				repetitionLevels[i] = ParquetType.INT64(0)
			}
		}

		var definitionLevels []interface{}
		if maxDefinitionLevel > 0 {
			bitWidth := Common.BitNum(uint64(maxDefinitionLevel))

			definitionLevels = ReadDataPageValues(bytesReader,
				pageHeader.DataPageHeader.GetDefinitionLevelEncoding(),
				parquet.Type_INT64,
				-1,
				uint64(pageHeader.DataPageHeader.GetNumValues()),
				bitWidth)

		} else {
			definitionLevels = make([]interface{}, pageHeader.DataPageHeader.GetNumValues())
			for i := 0; i < len(definitionLevels); i++ {
				definitionLevels[i] = ParquetType.INT64(0)
			}
		}

		var numNulls uint64 = 0
		for i := 0; i < len(definitionLevels); i++ {
			if int32(definitionLevels[i].(ParquetType.INT64)) != maxDefinitionLevel {
				numNulls++
			}
		}

		var values []interface{}
		var ct parquet.ConvertedType = -1
		if schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].IsSetConvertedType() {
			ct = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetConvertedType()
		}
		values = ReadDataPageValues(bytesReader,
			pageHeader.DataPageHeader.GetEncoding(),
			colMetaData.GetType(),
			ct,
			uint64(len(definitionLevels))-numNulls,
			uint64(schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetTypeLength()))

		table := new(Common.Table)
		table.Path = path
		table.RepetitionType = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetRepetitionType()
		table.MaxRepetitionLevel = maxRepetitionLevel
		table.MaxDefinitionLevel = maxDefinitionLevel
		table.Values = make([]interface{}, len(definitionLevels))
		table.RepetitionLevels = make([]int32, len(definitionLevels))
		table.DefinitionLevels = make([]int32, len(definitionLevels))

		j := 0
		numRows := int64(0)
		for i := 0; i < len(definitionLevels); i++ {
			dl, _ := definitionLevels[i].(ParquetType.INT64)
			rl, _ := repetitionLevels[i].(ParquetType.INT64)
			table.RepetitionLevels[i] = int32(rl)
			table.DefinitionLevels[i] = int32(dl)
			if table.DefinitionLevels[i] == maxDefinitionLevel {
				table.Values[i] = values[j]
				j++
			}
			if i+1 < len(definitionLevels) && table.RepetitionLevels[i+1] == 0 {
				numRows++
			}
		}
		page.DataTable = table

		return page, int64(len(definitionLevels)), numRows

	} else if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
		page = Layout.NewDictPage()
		page.Header = pageHeader
		table := new(Common.Table)
		table.Path = path
		table.Values = ParquetEncoding.ReadPlain(bytesReader,
			colMetaData.GetType(),
			-1,
			uint64(pageHeader.DictionaryPageHeader.GetNumValues()),
			0)
		page.DataTable = table
		return page, 0, 0

	} else if pageHeader.GetType() == parquet.PageType_INDEX_PAGE {

	} else if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		page = Layout.NewDataPage()
		page.Header = pageHeader
		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)

		var repetitionLevels []interface{}
		if maxRepetitionLevel > 0 {
			bitWidth := Common.BitNum(uint64(maxRepetitionLevel))

			repetitionLevels = ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				uint64(pageHeader.DataPageHeaderV2.GetNumValues()),
				bitWidth)

		} else {
			repetitionLevels = make([]interface{}, pageHeader.DataPageHeaderV2.GetNumValues())
			for i := 0; i < len(repetitionLevels); i++ {
				repetitionLevels[i] = ParquetType.INT64(0)
			}
		}

		var definitionLevels []interface{}
		if maxDefinitionLevel > 0 {
			bitWidth := Common.BitNum(uint64(maxDefinitionLevel))

			definitionLevels = ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				uint64(pageHeader.DataPageHeaderV2.GetNumValues()),
				bitWidth)

		} else {
			definitionLevels = make([]interface{}, pageHeader.DataPageHeaderV2.GetNumValues())
			for i := 0; i < len(definitionLevels); i++ {
				definitionLevels[i] = ParquetType.INT64(0)
			}
		}

		var numNulls uint64 = 0
		for i := 0; i < len(definitionLevels); i++ {
			if int32(definitionLevels[i].(ParquetType.INT64)) != maxDefinitionLevel {
				numNulls++
			}
		}

		var values []interface{}
		var ct parquet.ConvertedType = -1
		if schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].IsSetConvertedType() {
			ct = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetConvertedType()
		}
		values = ReadDataPageValues(bytesReader,
			pageHeader.DataPageHeaderV2.GetEncoding(),
			colMetaData.GetType(),
			ct,
			uint64(len(definitionLevels))-numNulls,
			uint64(schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetTypeLength()))

		table := new(Common.Table)
		table.Path = path
		table.RepetitionType = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetRepetitionType()
		table.MaxRepetitionLevel = maxRepetitionLevel
		table.MaxDefinitionLevel = maxDefinitionLevel
		table.Values = make([]interface{}, len(definitionLevels))
		table.RepetitionLevels = make([]int32, len(definitionLevels))
		table.DefinitionLevels = make([]int32, len(definitionLevels))

		j := 0
		numRows := int64(0)
		for i := 0; i < len(definitionLevels); i++ {
			dl, _ := definitionLevels[i].(ParquetType.INT64)
			rl, _ := repetitionLevels[i].(ParquetType.INT64)
			table.RepetitionLevels[i] = int32(rl)
			table.DefinitionLevels[i] = int32(dl)
			if table.DefinitionLevels[i] == maxDefinitionLevel {
				table.Values[i] = values[j]
				j++
			}
			if i+1 < len(definitionLevels) && table.RepetitionLevels[i+1] == 0 {
				numRows++
			}
		}
		page.DataTable = table

		return page, int64(len(definitionLevels)), numRows

	} else {
		log.Println("Error page type")
	}

	log.Println("Page Type Not Supported Yet")

	return nil, 0, 0

}
