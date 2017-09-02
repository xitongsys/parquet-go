package parquet_go

import (
	"bytes"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"parquet"
	"strings"
)

func ReadDataPageRaw(thriftReader *thrift.TBufferedTransport, colMetaData *parquet.ColumnMetaData, pageHeader *parquet.PageHeader) []byte {
	compressedPageSize := pageHeader.GetCompressedPageSize()
	buf := make([]byte, compressedPageSize)
	thriftReader.Read(buf)

	codec := colMetaData.GetCodec()
	if codec == parquet.CompressionCodec_GZIP {
		return UncompressGzip(buf)
	} else if codec == parquet.CompressionCodec_SNAPPY {
		return UncompressSnappy(buf)
	} else if codec == parquet.CompressionCodec_UNCOMPRESSED {
		return buf
	} else {
		log.Panicln("Unsupported Codec: ", codec)
	}
	return nil
}

func ReadDataPage(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler, colMetaData *parquet.ColumnMetaData, pageHeader *parquet.PageHeader, dictData []Interface) *Table {
	res := new(Table)

	dataPageHeader := pageHeader.GetDataPageHeader()
	rawBytes := ReadDataPageRaw(thriftReader, colMetaData, pageHeader)

	path := make([]string,0)
	path = append(path, schemaHandler.RootName)
	path = append(path, colMetaData.GetPathInSchema()...)
	maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(path)
	maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)

	bytesReader := bytes.NewReader(rawBytes)

	var definitionLevels []Interface
	if maxDefinitionLevel > 0 {
		bitWidth := WidthFromMaxInt(maxDefinitionLevel)
		definitionLevels = ReadData(bytesReader, dataPageHeader.GetDefinitionLevelEncoding(), int32(dataPageHeader.GetNumValues()), bitWidth)
	} else {
		definitionLevels = make([]Interface, dataPageHeader.GetNumValues())
		for i:=0; i<len(definitionLevels); i++ {
			definitionLevels[i] = int64(0)
		}
	}

	var numNULLs int32 = 0
	for i := 0; i < len(definitionLevels); i++ {
		if int32(definitionLevels[i].(int64)) != maxDefinitionLevel {
			numNULLs += 1
		}
	}

	var repetitionLevels []Interface
	if maxRepetitionLevel > 0 {
		bitWidth := WidthFromMaxInt(maxRepetitionLevel)
		repetitionLevels = ReadData(bytesReader, dataPageHeader.GetRepetitionLevelEncoding(), int32(dataPageHeader.GetNumValues()), bitWidth)
		
	} else {
		repetitionLevels = make([]Interface, dataPageHeader.GetNumValues())
		for i:=0; i<len(repetitionLevels); i++ {
			repetitionLevels[i] = int64(0)
		}
	}

	var values []Interface
	if dataPageHeader.GetEncoding() == parquet.Encoding_PLAIN {
		values = ReadPlain(bytesReader, colMetaData.GetType(), int32(len(definitionLevels))-numNULLs)
	} else if dataPageHeader.GetEncoding() == parquet.Encoding_PLAIN_DICTIONARY {
		b, _ := bytesReader.ReadByte()
		bitWidth := int32(b)
		valuesRaw := ReadRLEBitPackedHybrid(bytesReader, bitWidth, int32(bytesReader.Len()))
		ln := dataPageHeader.GetNumValues()
		values = make([]Interface, ln)
		if dictData == nil {
			log.Panicln("No DICT_PAGE found")
		} else {
			for i := 0; i < int(ln); i++ {
				values[i] = dictData[valuesRaw[i].(int64)]
			}
		}

	} else {
		log.Println("Not Konwn Enocding ", dataPageHeader.GetEncoding())
	}

	res.Path = path
	name := strings.Join(res.Path, ".")
	
	res.Repetition_Type = schemaHandler.SchemaMap[name].GetRepetitionType()
	res.MaxDefinitionLevel = maxDefinitionLevel
	res.MaxRepetitionLevel = maxDefinitionLevel

	res.Values = make([]Interface, len(definitionLevels))
	res.DefinitionLevels = make([]int32, len(definitionLevels))
	res.RepetitionLevels = make([]int32, len(definitionLevels))
	j := 0
	for i := 0; i < len(definitionLevels); i++ {
		dl, _ := definitionLevels[i].(int64)
		rl, _ := repetitionLevels[i].(int64)
		res.DefinitionLevels[i] = int32(dl)
		res.RepetitionLevels[i] = int32(rl)
		
		if res.DefinitionLevels[i] == maxDefinitionLevel {
			res.Values[i] = values[j]
			j++
		}
	}

	return res
}
