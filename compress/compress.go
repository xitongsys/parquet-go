package compress

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"

	"github.com/golang/snappy"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/DataDog/zstd"
)

//Uncompress using Gzip
func UncompressGzip(buf []byte) ([]byte, error) {
	rbuf := bytes.NewReader(buf)
	gzipReader, _ := gzip.NewReader(rbuf)
	res, err := ioutil.ReadAll(gzipReader)
	return res, err
}

//Compress using Gzip
func CompressGzip(buf []byte) []byte {
	var res bytes.Buffer
	gzipWriter := gzip.NewWriter(&res)
	gzipWriter.Write(buf)
	gzipWriter.Close()
	return res.Bytes()
}

//Uncompress using Snappy
func UncompressSnappy(buf []byte) ([]byte, error) {
	return snappy.Decode(nil, buf)
}

//Compress using Snappy
func CompressSnappy(buf []byte) []byte {
	return snappy.Encode(nil, buf)
}

//Compress using Zstd
func CompressZstd(buf []byte) []byte {
	res, _ := zstd.Compress(nil, buf)
	return res
}

//Uncompress using Zstd
func UncompressZstd(buf []byte) ([]byte, error) {
	return zstd.Decompress(nil, buf)
}

func Uncompress(buf []byte, compressMethod parquet.CompressionCodec) ([]byte, error) {
	switch compressMethod {
	case parquet.CompressionCodec_GZIP:
		return UncompressGzip(buf)
	case parquet.CompressionCodec_SNAPPY:
		return UncompressSnappy(buf)
	case parquet.CompressionCodec_ZSTD:
		return UncompressZstd(buf)
	case parquet.CompressionCodec_UNCOMPRESSED:
		return buf, nil
	default:
		return nil, fmt.Errorf("Unsupported compress method")
	}
}

func Compress(buf []byte, compressMethod parquet.CompressionCodec) []byte {
	switch compressMethod {
	case parquet.CompressionCodec_GZIP:
		return CompressGzip(buf)
	case parquet.CompressionCodec_SNAPPY:
		return CompressSnappy(buf)
	case parquet.CompressionCodec_ZSTD:
		return CompressZstd(buf)
	case parquet.CompressionCodec_UNCOMPRESSED:
		return buf
	default:
		return nil
	}
}
