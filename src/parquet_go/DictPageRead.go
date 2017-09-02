package parquet_go

import (
	"bytes"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"parquet"
)

func ReadDictPageRaw(thriftReader *thrift.TBufferedTransport, colMetaData *parquet.ColumnMetaData, pageHeader *parquet.PageHeader) []byte {
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

func ReadDictPage(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler, colMetaData *parquet.ColumnMetaData, pageHeader *parquet.PageHeader) []Interface {
	dictPageHeader := pageHeader.GetDictionaryPageHeader()
	rawBytes := ReadDataPageRaw(thriftReader, colMetaData, pageHeader)
	bytesReader := bytes.NewReader(rawBytes)
	res := ReadPlain(bytesReader, colMetaData.GetType(), int32(dictPageHeader.GetNumValues()))
	return res
}
