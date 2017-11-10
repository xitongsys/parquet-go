package ParquetHandler

import (
	"bytes"
	"git.apache.org/thrift.git/lib/go/thrift"
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/Compress"
	. "github.com/xitongsys/parquet-go/Layout"
	. "github.com/xitongsys/parquet-go/ParquetEncoding"
	. "github.com/xitongsys/parquet-go/ParquetType"
	. "github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"log"
	"strings"
)

func (self *ParquetHandler) ReadPageHeader(thriftReader *thrift.TBufferedTransport) *parquet.PageHeader {
	protocol := thrift.NewTCompactProtocol(thriftReader)
	pageHeader := parquet.NewPageHeader()
	pageHeader.Read(protocol)
	return pageHeader
}

func (self *ParquetHandler) ReadDataPageValues(bytesReader *bytes.Reader, encoding parquet.Encoding, dataType parquet.Type, convertedType parquet.ConvertedType, cnt uint64, bitWidth uint64) []interface{} {
	if encoding == parquet.Encoding_PLAIN {
		return ReadPlain(bytesReader, dataType, convertedType, cnt, bitWidth)

	} else if encoding == parquet.Encoding_PLAIN_DICTIONARY {
		b, _ := bytesReader.ReadByte()
		bitWidth = uint64(b)
		return ReadRLEBitPackedHybrid(bytesReader, bitWidth, uint64(bytesReader.Len()))

	} else if encoding == parquet.Encoding_RLE {
		values := ReadRLEBitPackedHybrid(bytesReader, bitWidth, 0)
		if dataType == parquet.Type_INT32 {
			for i := 0; i < len(values); i++ {
				values[i] = INT32(values[i].(INT64))
			}
		}
		return values

	} else if encoding == parquet.Encoding_BIT_PACKED {
		//deprecated
	} else if encoding == parquet.Encoding_DELTA_BINARY_PACKED {
		values := ReadDeltaBinaryPackedINT(bytesReader)
		if dataType == parquet.Type_INT32 {
			for i := 0; i < len(values); i++ {
				values[i] = INT32(values[i].(INT64))
			}
		}
		return values

	} else if encoding == parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY {
		values := ReadDeltaLengthByteArray(bytesReader)
		if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			for i := 0; i < len(values); i++ {
				values[i] = FIXED_LEN_BYTE_ARRAY(values[i].(BYTE_ARRAY))
			}
		}
		return values

	} else if encoding == parquet.Encoding_DELTA_BYTE_ARRAY {
		values := ReadDeltaByteArray(bytesReader)
		if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			for i := 0; i < len(values); i++ {
				values[i] = FIXED_LEN_BYTE_ARRAY(values[i].(BYTE_ARRAY))
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

func (self *ParquetHandler) ReadPage(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Page, int64) {
	pageHeader := self.ReadPageHeader(thriftReader)

	buf := make([]byte, 0)

	var page *Page
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
			dataBuf = UncompressGzip(dataBuf)
		} else if codec == parquet.CompressionCodec_SNAPPY {
			dataBuf = UncompressSnappy(dataBuf)
		} else if codec == parquet.CompressionCodec_UNCOMPRESSED {
			dataBuf = dataBuf
		} else {
			log.Panicln("Unsupported Codec: ", codec)
		}
		tmpBuf := make([]byte, 0)
		if rll > 0 {
			tmpBuf = WritePlainINT32([]interface{}{INT32(rll)})
			tmpBuf = append(tmpBuf, repetitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		if dll > 0 {
			tmpBuf = WritePlainINT32([]interface{}{INT32(dll)})
			tmpBuf = append(tmpBuf, definitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		buf = append(buf, dataBuf...)

	} else {
		buf = make([]byte, compressedPageSize)
		thriftReader.Read(buf)
		codec := colMetaData.GetCodec()
		if codec == parquet.CompressionCodec_GZIP {
			buf = UncompressGzip(buf)
		} else if codec == parquet.CompressionCodec_SNAPPY {
			buf = UncompressSnappy(buf)
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
		page = NewDataPage()
		page.Header = pageHeader
		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)

		var repetitionLevels []interface{}
		if maxRepetitionLevel > 0 {
			bitWidth := BitNum(uint64(maxRepetitionLevel))

			repetitionLevels = self.ReadDataPageValues(bytesReader,
				pageHeader.DataPageHeader.GetRepetitionLevelEncoding(),
				parquet.Type_INT64,
				-1,
				uint64(pageHeader.DataPageHeader.GetNumValues()),
				bitWidth)

		} else {
			repetitionLevels = make([]interface{}, pageHeader.DataPageHeader.GetNumValues())
			for i := 0; i < len(repetitionLevels); i++ {
				repetitionLevels[i] = INT64(0)
			}
		}

		var definitionLevels []interface{}
		if maxDefinitionLevel > 0 {
			bitWidth := BitNum(uint64(maxDefinitionLevel))

			definitionLevels = self.ReadDataPageValues(bytesReader,
				pageHeader.DataPageHeader.GetDefinitionLevelEncoding(),
				parquet.Type_INT64,
				-1,
				uint64(pageHeader.DataPageHeader.GetNumValues()),
				bitWidth)

		} else {
			definitionLevels = make([]interface{}, pageHeader.DataPageHeader.GetNumValues())
			for i := 0; i < len(definitionLevels); i++ {
				definitionLevels[i] = INT64(0)
			}
		}

		var numNulls uint64 = 0
		for i := 0; i < len(definitionLevels); i++ {
			if int32(definitionLevels[i].(INT64)) != maxDefinitionLevel {
				numNulls++
			}
		}

		var values []interface{}
		var ct parquet.ConvertedType = -1
		if schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].IsSetConvertedType() {
			ct = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetConvertedType()
		}
		values = self.ReadDataPageValues(bytesReader,
			pageHeader.DataPageHeader.GetEncoding(),
			colMetaData.GetType(),
			ct,
			uint64(len(definitionLevels))-numNulls,
			uint64(schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetTypeLength()))

		table := new(Table)
		table.Path = path
		table.Repetition_Type = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetRepetitionType()
		table.MaxRepetitionLevel = maxRepetitionLevel
		table.MaxDefinitionLevel = maxDefinitionLevel
		table.Values = make([]interface{}, len(definitionLevels))
		table.RepetitionLevels = make([]int32, len(definitionLevels))
		table.DefinitionLevels = make([]int32, len(definitionLevels))

		j := 0
		for i := 0; i < len(definitionLevels); i++ {
			dl, _ := definitionLevels[i].(INT64)
			rl, _ := repetitionLevels[i].(INT64)
			table.RepetitionLevels[i] = int32(rl)
			table.DefinitionLevels[i] = int32(dl)
			if table.DefinitionLevels[i] == maxDefinitionLevel {
				table.Values[i] = values[j]
				j++
			}
		}
		page.DataTable = table

		return page, int64(len(definitionLevels))

	} else if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
		page = NewDictPage()
		page.Header = pageHeader
		table := new(Table)
		table.Path = path
		table.Values = ReadPlain(bytesReader,
			colMetaData.GetType(),
			-1,
			uint64(pageHeader.DictionaryPageHeader.GetNumValues()),
			0)
		page.DataTable = table
		return page, 0

	} else if pageHeader.GetType() == parquet.PageType_INDEX_PAGE {

	} else if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		page = NewDataPage()
		page.Header = pageHeader
		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)

		var repetitionLevels []interface{}
		if maxRepetitionLevel > 0 {
			bitWidth := BitNum(uint64(maxRepetitionLevel))

			repetitionLevels = self.ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				uint64(pageHeader.DataPageHeaderV2.GetNumValues()),
				bitWidth)

		} else {
			repetitionLevels = make([]interface{}, pageHeader.DataPageHeaderV2.GetNumValues())
			for i := 0; i < len(repetitionLevels); i++ {
				repetitionLevels[i] = INT64(0)
			}
		}

		var definitionLevels []interface{}
		if maxDefinitionLevel > 0 {
			bitWidth := BitNum(uint64(maxDefinitionLevel))

			definitionLevels = self.ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				uint64(pageHeader.DataPageHeaderV2.GetNumValues()),
				bitWidth)

		} else {
			definitionLevels = make([]interface{}, pageHeader.DataPageHeaderV2.GetNumValues())
			for i := 0; i < len(definitionLevels); i++ {
				definitionLevels[i] = INT64(0)
			}
		}

		var numNulls uint64 = 0
		for i := 0; i < len(definitionLevels); i++ {
			if int32(definitionLevels[i].(INT64)) != maxDefinitionLevel {
				numNulls++
			}
		}

		var values []interface{}
		var ct parquet.ConvertedType = -1
		if schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].IsSetConvertedType() {
			ct = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetConvertedType()
		}
		values = self.ReadDataPageValues(bytesReader,
			pageHeader.DataPageHeaderV2.GetEncoding(),
			colMetaData.GetType(),
			ct,
			uint64(len(definitionLevels))-numNulls,
			uint64(schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetTypeLength()))

		table := new(Table)
		table.Path = path
		table.Repetition_Type = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetRepetitionType()
		table.MaxRepetitionLevel = maxRepetitionLevel
		table.MaxDefinitionLevel = maxDefinitionLevel
		table.Values = make([]interface{}, len(definitionLevels))
		table.RepetitionLevels = make([]int32, len(definitionLevels))
		table.DefinitionLevels = make([]int32, len(definitionLevels))

		j := 0
		for i := 0; i < len(definitionLevels); i++ {
			dl, _ := definitionLevels[i].(INT64)
			rl, _ := repetitionLevels[i].(INT64)
			table.RepetitionLevels[i] = int32(rl)
			table.DefinitionLevels[i] = int32(dl)
			if table.DefinitionLevels[i] == maxDefinitionLevel {
				table.Values[i] = values[j]
				j++
			}
		}
		page.DataTable = table

		return page, int64(len(definitionLevels))

	} else {
		log.Println("Error page type")
	}

	log.Println("Page Type Not Supported Yet")

	return nil, 0

}
