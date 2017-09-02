package parquet_go

import (
	"bytes"
	"compress/gzip"
	"github.com/golang/snappy"
	"io/ioutil"
	"log"
)

func UncompressGzip(buf []byte) []byte {
	rbuf := bytes.NewReader(buf)
	gzipReader, _ := gzip.NewReader(rbuf)
	res, _ := ioutil.ReadAll(gzipReader)
	return res
}

func CompressGzip(buf []byte) []byte {
	var res bytes.Buffer
	gzipWriter := gzip.NewWriter(&res)
	gzipWriter.Write(buf)
	gzipWriter.Close()
	return res.Bytes()
}

func UncompressSnappy(buf []byte) []byte {
	res, err := snappy.Decode(nil, buf)
	if err != nil {
		log.Println("UncompressSnappy Error")
	}
	return res
}

func CompressSnappy(buf []byte) []byte {
	return snappy.Encode(nil, buf)
}
