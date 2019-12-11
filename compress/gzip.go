// +build !no_gzip

package compress

import (
	"bytes"
	"github.com/klauspost/compress/gzip"
	"github.com/xitongsys/parquet-go/parquet"
	"io/ioutil"
)

func init() {
	compressors[parquet.CompressionCodec_GZIP] = &Compressor{
		Compress: func(buf []byte) []byte {
			var res bytes.Buffer
			gzipWriter := gzip.NewWriter(&res)
			gzipWriter.Write(buf)
			gzipWriter.Close()
			return res.Bytes()
		},
		Uncompress: func(buf []byte) (i []byte, err error) {
			rbuf := bytes.NewReader(buf)
			gzipReader, _ := gzip.NewReader(rbuf)
			res, err := ioutil.ReadAll(gzipReader)
			return res, err
		},
	}
}
