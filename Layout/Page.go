package Layout

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Compress"
	"github.com/xitongsys/parquet-go/ParquetEncoding"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

//Page is used to store the page data
type Page struct {
	//Header of a page
	Header *parquet.PageHeader
	//Table to store values
	DataTable *Table
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
	//Tag info
	Info *Common.Tag

	PageSize int32
}

//Create a new page
func NewPage() *Page {
	page := new(Page)
	page.DataTable = nil
	page.Header = parquet.NewPageHeader()
	page.Info = Common.NewTag()
	page.PageSize = 8 * 1024
	return page
}

//Create a new dict page
func NewDictPage() *Page {
	page := NewPage()
	page.Header.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	page.PageSize = 8 * 1024
	return page
}

//Create a new data page
func NewDataPage() *Page {
	page := NewPage()
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	page.PageSize = 8 * 1024
	return page
}

//Convert a table to data pages
func TableToDataPages(table *Table, pageSize int32, compressType parquet.CompressionCodec) ([]*Page, int64) {
	var totSize int64 = 0
	totalLn := len(table.Values)
	res := make([]*Page, 0)
	i := 0
	dataType := table.Type
	pT, cT := ParquetType.TypeNameToParquetType(table.Info.Type, table.Info.BaseType)

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
				maxVal = Common.Max(maxVal, table.Values[j], pT, cT)
				minVal = Common.Min(minVal, table.Values[j], pT, cT)
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
		page.MaxVal = maxVal
		page.MinVal = minVal
		page.DataType = dataType
		page.CompressType = compressType
		page.Path = table.Path
		page.Info = table.Info

		page.DataPageCompress(compressType)

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = j
	}
	return res, totSize
}

//Decode dict page
func (page *Page) Decode(dictPage *Page) {
	if dictPage == nil {
		return
	}

	if page == nil || page.Header.DataPageHeader == nil ||
		(page.Header.DataPageHeader.Encoding != parquet.Encoding_RLE_DICTIONARY &&
			page.Header.DataPageHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY) {
		return
	}

	numValues := len(page.DataTable.Values)
	for i := 0; i < numValues; i++ {
		if page.DataTable.Values[i] != nil {
			index := page.DataTable.Values[i].(int64)
			page.DataTable.Values[i] = dictPage.DataTable.Values[index]
		}
	}
}

//Encoding values
func (page *Page) EncodingValues(valuesBuf []interface{}) []byte {
	encoding := parquet.Encoding_PLAIN
	if page.Info.Encoding != 0 {
		encoding = page.Info.Encoding
	}
	if encoding == parquet.Encoding_RLE {
		bitWidth := page.Info.Length
		return ParquetEncoding.WriteRLEBitPackedHybrid(valuesBuf, bitWidth, page.DataType)

	} else if encoding == parquet.Encoding_DELTA_BINARY_PACKED {
		return ParquetEncoding.WriteDelta(valuesBuf)

	} else if encoding == parquet.Encoding_DELTA_BYTE_ARRAY {
		return ParquetEncoding.WriteDeltaByteArray(valuesBuf)

	} else if encoding == parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY {
		return ParquetEncoding.WriteDeltaLengthByteArray(valuesBuf)

	} else {
		return ParquetEncoding.WritePlain(valuesBuf, page.DataType)
	}
	return []byte{}
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
	//valuesRawBuf := ParquetEncoding.WritePlain(valuesBuf)
	valuesRawBuf := page.EncodingValues(valuesBuf)

	//definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	if page.DataTable.MaxDefinitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = int64(page.DataTable.DefinitionLevels[i])
		}
		definitionLevelBuf = ParquetEncoding.WriteRLEBitPackedHybrid(numInterfaces,
			int32(Common.BitNum(uint64(page.DataTable.MaxDefinitionLevel))),
			parquet.Type_INT64)
	}

	//repetitionLevel/////////////////////////////////
	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = int64(page.DataTable.RepetitionLevels[i])
		}
		repetitionLevelBuf = ParquetEncoding.WriteRLEBitPackedHybrid(numInterfaces,
			int32(Common.BitNum(uint64(page.DataTable.MaxRepetitionLevel))),
			parquet.Type_INT64)
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
	page.Header.DataPageHeader.NumValues = int32(len(page.DataTable.DefinitionLevels))
	page.Header.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.Encoding = page.Info.Encoding

	page.Header.DataPageHeader.Statistics = parquet.NewStatistics()
	if page.MaxVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MaxVal}, page.DataType)
		name := page.Info.Type
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Max = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MinVal}, page.DataType)
		name := page.Info.Type
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Min = tmpBuf
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(context.TODO(), page.Header)

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
	//valuesRawBuf := ParquetEncoding.WritePlain(valuesBuf)
	valuesRawBuf := page.EncodingValues(valuesBuf)

	//definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	if page.DataTable.MaxDefinitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = int64(page.DataTable.DefinitionLevels[i])
		}
		definitionLevelBuf = ParquetEncoding.WriteRLE(numInterfaces,
			int32(Common.BitNum(uint64(page.DataTable.MaxDefinitionLevel))),
			parquet.Type_INT64)
	}

	//repetitionLevel/////////////////////////////////
	r0Num := int32(0)
	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = int64(page.DataTable.RepetitionLevels[i])
			if page.DataTable.RepetitionLevels[i] == 0 {
				r0Num++
			}
		}
		repetitionLevelBuf = ParquetEncoding.WriteRLE(numInterfaces,
			int32(Common.BitNum(uint64(page.DataTable.MaxRepetitionLevel))),
			parquet.Type_INT64)
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
	//page.Header.DataPageHeaderV2.Encoding = parquet.Encoding_PLAIN
	page.Header.DataPageHeaderV2.Encoding = page.Info.Encoding

	page.Header.DataPageHeaderV2.DefinitionLevelsByteLength = int32(len(definitionLevelBuf))
	page.Header.DataPageHeaderV2.RepetitionLevelsByteLength = int32(len(repetitionLevelBuf))
	page.Header.DataPageHeaderV2.IsCompressed = true

	page.Header.DataPageHeaderV2.Statistics = parquet.NewStatistics()
	if page.MaxVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MaxVal}, page.DataType)
		//name := reflect.TypeOf(page.MaxVal).Name()
		name := page.Info.Type
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeaderV2.Statistics.Max = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MinVal}, page.DataType)
		//name := reflect.TypeOf(page.MinVal).Name()
		name := page.Info.Type
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeaderV2.Statistics.Min = tmpBuf
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(context.TODO(), page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, repetitionLevelBuf...)
	res = append(res, definitionLevelBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res

	return res
}

//This is a test function
func ReadPage2(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler.SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Page, int64, int64, error) {
	var err error
	page, err := ReadPageRawData(thriftReader, schemaHandler, colMetaData)
	if err != nil {
		return nil, 0, 0, err
	}
	numValues, numRows, err := page.GetRLDLFromRawData(schemaHandler)
	if err != nil {
		return nil, 0, 0, err
	}
	if err = page.GetValueFromRawData(schemaHandler); err != nil {
		return page, 0, 0, err
	}
	return page, numValues, numRows, nil
}

//Read page RawData
func ReadPageRawData(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler.SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Page, error) {
	var (
		err error
	)

	pageHeader, err := ReadPageHeader(thriftReader)
	if err != nil {
		return nil, err
	}

	var page *Page
	if pageHeader.GetType() == parquet.PageType_DATA_PAGE || pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		page = NewDataPage()
	} else if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
		page = NewDictPage()
	} else {
		return page, fmt.Errorf("Unsupported page type")
	}

	compressedPageSize := pageHeader.GetCompressedPageSize()
	buf := make([]byte, compressedPageSize)
	if _, err := thriftReader.Read(buf); err != nil {
		return nil, err
	}

	page.Header = pageHeader
	page.CompressType = colMetaData.GetCodec()
	page.RawData = buf
	page.Path = make([]string, 0)
	page.Path = append(page.Path, schemaHandler.GetRootName())
	page.Path = append(page.Path, colMetaData.GetPathInSchema()...)
	page.DataType = colMetaData.GetType()
	return page, nil
}

//Get RepetitionLevels and Definitions from RawData
func (self *Page) GetRLDLFromRawData(schemaHandler *SchemaHandler.SchemaHandler) (int64, int64, error) {
	var err error
	bytesReader := bytes.NewReader(self.RawData)
	buf := make([]byte, 0)

	if self.Header.GetType() == parquet.PageType_DATA_PAGE_V2 {
		dll := self.Header.DataPageHeaderV2.GetDefinitionLevelsByteLength()
		rll := self.Header.DataPageHeaderV2.GetRepetitionLevelsByteLength()
		repetitionLevelsBuf, definitionLevelsBuf := make([]byte, rll), make([]byte, dll)
		dataBuf := make([]byte, len(self.RawData)-int(rll)-int(dll))
		bytesReader.Read(repetitionLevelsBuf)
		bytesReader.Read(definitionLevelsBuf)
		bytesReader.Read(dataBuf)

		tmpBuf := make([]byte, 0)
		if rll > 0 {
			tmpBuf = ParquetEncoding.WritePlainINT32([]interface{}{int32(rll)})
			tmpBuf = append(tmpBuf, repetitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		if dll > 0 {
			tmpBuf = ParquetEncoding.WritePlainINT32([]interface{}{int32(dll)})
			tmpBuf = append(tmpBuf, definitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		buf = append(buf, dataBuf...)

	} else {
		if buf, err = Compress.Uncompress(self.RawData, self.CompressType); err != nil {
			return 0, 0, fmt.Errorf("Unsupported compress method")
		}
	}

	bytesReader = bytes.NewReader(buf)
	if self.Header.GetType() == parquet.PageType_DATA_PAGE_V2 || self.Header.GetType() == parquet.PageType_DATA_PAGE {
		var numValues uint64
		if self.Header.GetType() == parquet.PageType_DATA_PAGE {
			numValues = uint64(self.Header.DataPageHeader.GetNumValues())
		} else {
			numValues = uint64(self.Header.DataPageHeaderV2.GetNumValues())
		}

		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(self.Path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(self.Path)

		var repetitionLevels, definitionLevels []interface{}
		if maxRepetitionLevel > 0 {
			bitWidth := Common.BitNum(uint64(maxRepetitionLevel))
			if repetitionLevels, err = ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth); err != nil {
				return 0, 0, err
			}
		} else {
			repetitionLevels = make([]interface{}, numValues)
			for i := 0; i < len(repetitionLevels); i++ {
				repetitionLevels[i] = int64(0)
			}
		}
		if len(repetitionLevels) > int(numValues) {
			repetitionLevels = repetitionLevels[:numValues]
		}

		if maxDefinitionLevel > 0 {
			bitWidth := Common.BitNum(uint64(maxDefinitionLevel))

			definitionLevels, err = ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth)
			if err != nil {
				return 0, 0, err
			}

		} else {
			definitionLevels = make([]interface{}, numValues)
			for i := 0; i < len(definitionLevels); i++ {
				definitionLevels[i] = int64(0)
			}
		}
		if len(definitionLevels) > int(numValues) {
			definitionLevels = definitionLevels[:numValues]
		}

		table := new(Table)
		table.Path = self.Path
		name := strings.Join(self.Path, ".")
		table.RepetitionType = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetRepetitionType()
		table.MaxRepetitionLevel = maxRepetitionLevel
		table.MaxDefinitionLevel = maxDefinitionLevel
		table.Values = make([]interface{}, len(definitionLevels))
		table.RepetitionLevels = make([]int32, len(definitionLevels))
		table.DefinitionLevels = make([]int32, len(definitionLevels))

		numRows := int64(0)
		for i := 0; i < len(definitionLevels); i++ {
			dl, _ := definitionLevels[i].(int64)
			rl, _ := repetitionLevels[i].(int64)
			table.RepetitionLevels[i] = int32(rl)
			table.DefinitionLevels[i] = int32(dl)
			if table.RepetitionLevels[i] == 0 {
				numRows++
			}
		}
		self.DataTable = table
		self.RawData = buf[len(buf)-bytesReader.Len():]

		return int64(numValues), numRows, nil

	} else if self.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		table := new(Table)
		table.Path = self.Path
		self.DataTable = table
		return 0, 0, nil

	} else {
		return 0, 0, fmt.Errorf("Unsupported page type")
	}
}

//Get values from raw data
func (self *Page) GetValueFromRawData(schemaHandler *SchemaHandler.SchemaHandler) error {
	var err error
	var encodingType parquet.Encoding

	switch self.Header.GetType() {
	case parquet.PageType_DICTIONARY_PAGE:
		bytesReader := bytes.NewReader(self.RawData)
		self.DataTable.Values, err = ParquetEncoding.ReadPlain(bytesReader,
			self.DataType,
			uint64(self.Header.DictionaryPageHeader.GetNumValues()),
			0)
		if err != nil {
			return err
		}
	case parquet.PageType_DATA_PAGE_V2:
		if self.RawData, err = Compress.Uncompress(self.RawData, self.CompressType); err != nil {
			return err
		}
		encodingType = self.Header.DataPageHeader.GetEncoding()
		fallthrough
	case parquet.PageType_DATA_PAGE:
		encodingType = self.Header.DataPageHeader.GetEncoding()
		bytesReader := bytes.NewReader(self.RawData)

		var numNulls uint64 = 0
		for i := 0; i < len(self.DataTable.DefinitionLevels); i++ {
			if self.DataTable.DefinitionLevels[i] != self.DataTable.MaxDefinitionLevel {
				numNulls++
			}
		}
		name := strings.Join(self.DataTable.Path, ".")
		var values []interface{}
		var ct parquet.ConvertedType = -1
		if schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].IsSetConvertedType() {
			ct = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetConvertedType()
		}

		values, err = ReadDataPageValues(bytesReader,
			encodingType,
			self.DataType,
			ct,
			uint64(len(self.DataTable.DefinitionLevels))-numNulls,
			uint64(schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetTypeLength()))
		if err != nil {
			return err
		}
		j := 0
		for i := 0; i < len(self.DataTable.DefinitionLevels); i++ {
			if self.DataTable.DefinitionLevels[i] == self.DataTable.MaxDefinitionLevel {
				self.DataTable.Values[i] = values[j]
				j++
			}
		}
		self.RawData = []byte{}
		return nil

	default:
		return fmt.Errorf("Unsupported page type")
	}
	return nil
}

//Read page header
func ReadPageHeader(thriftReader *thrift.TBufferedTransport) (*parquet.PageHeader, error) {
	protocol := thrift.NewTCompactProtocol(thriftReader)
	pageHeader := parquet.NewPageHeader()
	err := pageHeader.Read(protocol)
	return pageHeader, err
}

//Read data page values
func ReadDataPageValues(bytesReader *bytes.Reader, encoding parquet.Encoding, dataType parquet.Type, convertedType parquet.ConvertedType, cnt uint64, bitWidth uint64) ([]interface{}, error) {
	var (
		res []interface{}
	)

	if encoding == parquet.Encoding_PLAIN {
		return ParquetEncoding.ReadPlain(bytesReader, dataType, cnt, bitWidth)

	} else if encoding == parquet.Encoding_PLAIN_DICTIONARY {
		b, err := bytesReader.ReadByte()
		if err != nil {
			return res, err
		}
		bitWidth = uint64(b)
		if bytesReader.Len() == 0 {
			return res, nil
		}
		buf, err := ParquetEncoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, uint64(bytesReader.Len()))
		if err != nil {
			return res, err
		}
		return buf[:cnt], err

	} else if encoding == parquet.Encoding_RLE {
		values, err := ParquetEncoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, 0)
		if err != nil {
			return res, err
		}
		if dataType == parquet.Type_INT32 {
			for i := 0; i < len(values); i++ {
				values[i] = int32(values[i].(int64))
			}
		}
		return values[:cnt], nil

	} else if encoding == parquet.Encoding_BIT_PACKED {
		//deprecated
		return res, fmt.Errorf("Unsupported Encoding method BIT_PACKED")

	} else if encoding == parquet.Encoding_DELTA_BINARY_PACKED {
		values, err := ParquetEncoding.ReadDeltaBinaryPackedINT(bytesReader)
		if err != nil {
			return res, err
		}
		if dataType == parquet.Type_INT32 {
			for i := 0; i < len(values); i++ {
				values[i] = int32(values[i].(int64))
			}
		}
		return values[:cnt], nil

	} else if encoding == parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY {
		values, err := ParquetEncoding.ReadDeltaLengthByteArray(bytesReader)
		if err != nil {
			return res, err
		}
		if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			for i := 0; i < len(values); i++ {
				values[i] = values[i].(string)
			}
		}
		return values[:cnt], nil

	} else if encoding == parquet.Encoding_DELTA_BYTE_ARRAY {
		values, err := ParquetEncoding.ReadDeltaByteArray(bytesReader)
		if err != nil {
			return res, err
		}
		if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			for i := 0; i < len(values); i++ {
				values[i] = values[i].(string)
			}
		}
		return values[:cnt], nil

	} else {
		return res, fmt.Errorf("Unknown Encoding method")
	}
}

//Read page from parquet file
func ReadPage(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler.SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Page, int64, int64, error) {
	var (
		err error
	)

	pageHeader, err := ReadPageHeader(thriftReader)
	if err != nil {
		return nil, 0, 0, err
	}

	buf := make([]byte, 0)

	var page *Page
	compressedPageSize := pageHeader.GetCompressedPageSize()

	if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		dll := pageHeader.DataPageHeaderV2.GetDefinitionLevelsByteLength()
		rll := pageHeader.DataPageHeaderV2.GetRepetitionLevelsByteLength()
		repetitionLevelsBuf := make([]byte, rll)
		definitionLevelsBuf := make([]byte, dll)
		dataBuf := make([]byte, compressedPageSize-rll-dll)

		if _, err = thriftReader.Read(repetitionLevelsBuf); err != nil {
			return nil, 0, 0, err
		}
		if _, err = thriftReader.Read(definitionLevelsBuf); err != nil {
			return nil, 0, 0, err
		}
		if _, err = thriftReader.Read(dataBuf); err != nil {
			return nil, 0, 0, err
		}

		codec := colMetaData.GetCodec()
		if dataBuf, err = Compress.Uncompress(dataBuf, codec); err != nil {
			return nil, 0, 0, err
		}

		tmpBuf := make([]byte, 0)
		if rll > 0 {
			tmpBuf = ParquetEncoding.WritePlainINT32([]interface{}{int32(rll)})
			tmpBuf = append(tmpBuf, repetitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		if dll > 0 {
			tmpBuf = ParquetEncoding.WritePlainINT32([]interface{}{int32(dll)})
			tmpBuf = append(tmpBuf, definitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		buf = append(buf, dataBuf...)

	} else {
		buf = make([]byte, compressedPageSize)
		if _, err = thriftReader.Read(buf); err != nil {
			return nil, 0, 0, err
		}
		codec := colMetaData.GetCodec()
		if buf, err = Compress.Uncompress(buf, codec); err != nil {
			return nil, 0, 0, err
		}
	}

	bytesReader := bytes.NewReader(buf)
	path := make([]string, 0)
	path = append(path, schemaHandler.GetRootName())
	path = append(path, colMetaData.GetPathInSchema()...)
	name := strings.Join(path, ".")

	if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
		page = NewDictPage()
		page.Header = pageHeader
		table := new(Table)
		table.Path = path
		table.Values, err = ParquetEncoding.ReadPlain(bytesReader,
			colMetaData.GetType(),
			uint64(pageHeader.DictionaryPageHeader.GetNumValues()),
			0)
		if err != nil {
			return nil, 0, 0, err
		}
		page.DataTable = table

		return page, 0, 0, nil

	} else if pageHeader.GetType() == parquet.PageType_INDEX_PAGE {
		return nil, 0, 0, fmt.Errorf("Unsupported page type: INDEX_PAGE")

	} else if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 ||
		pageHeader.GetType() == parquet.PageType_DATA_PAGE {

		page = NewDataPage()
		page.Header = pageHeader
		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)

		var numValues uint64
		var encodingType parquet.Encoding

		if pageHeader.GetType() == parquet.PageType_DATA_PAGE {
			numValues = uint64(pageHeader.DataPageHeader.GetNumValues())
			encodingType = pageHeader.DataPageHeader.GetEncoding()
		} else {
			numValues = uint64(pageHeader.DataPageHeaderV2.GetNumValues())
			encodingType = pageHeader.DataPageHeaderV2.GetEncoding()
		}

		var repetitionLevels []interface{}
		if maxRepetitionLevel > 0 {
			bitWidth := Common.BitNum(uint64(maxRepetitionLevel))

			repetitionLevels, err = ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth)
			if err != nil {
				return nil, 0, 0, err
			}

		} else {
			repetitionLevels = make([]interface{}, numValues)
			for i := 0; i < len(repetitionLevels); i++ {
				repetitionLevels[i] = int64(0)
			}
		}
		if len(repetitionLevels) > int(numValues) {
			repetitionLevels = repetitionLevels[:numValues]
		}

		var definitionLevels []interface{}
		if maxDefinitionLevel > 0 {
			bitWidth := Common.BitNum(uint64(maxDefinitionLevel))

			definitionLevels, err = ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth)
			if err != nil {
				return nil, 0, 0, err
			}

		} else {
			definitionLevels = make([]interface{}, numValues)
			for i := 0; i < len(definitionLevels); i++ {
				definitionLevels[i] = int64(0)
			}
		}
		if len(definitionLevels) > int(numValues) {
			definitionLevels = definitionLevels[:numValues]
		}

		var numNulls uint64 = 0
		for i := 0; i < len(definitionLevels); i++ {
			if int32(definitionLevels[i].(int64)) != maxDefinitionLevel {
				numNulls++
			}
		}

		var values []interface{}
		var ct parquet.ConvertedType = -1
		if schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].IsSetConvertedType() {
			ct = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetConvertedType()
		}
		values, err = ReadDataPageValues(bytesReader,
			encodingType,
			colMetaData.GetType(),
			ct,
			uint64(len(definitionLevels))-numNulls,
			uint64(schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetTypeLength()))
		if err != nil {
			return nil, 0, 0, err
		}

		table := new(Table)
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
			dl, _ := definitionLevels[i].(int64)
			rl, _ := repetitionLevels[i].(int64)
			table.RepetitionLevels[i] = int32(rl)
			table.DefinitionLevels[i] = int32(dl)
			if table.DefinitionLevels[i] == maxDefinitionLevel {
				table.Values[i] = values[j]
				j++
			}
			if table.RepetitionLevels[i] == 0 {
				numRows++
			}
		}
		page.DataTable = table

		return page, int64(len(definitionLevels)), numRows, nil

	} else {
		return nil, 0, 0, fmt.Errorf("Error page type %v", pageHeader.GetType())
	}

}
