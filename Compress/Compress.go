package Compress

import (
	"bytes"
	"compress/gzip"
	"github.com/golang/snappy"
	"io/ioutil"
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
