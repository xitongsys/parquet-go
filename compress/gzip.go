// +build !no_gzip

package compress

import (
	"bytes"
	"compress/gzip"
	"github.com/xitongsys/parquet-go/parquet"
	"io/ioutil"
	"sync"
)

var gzipWriterPool sync.Pool
var buffersPool sync.Pool

func init() {
	gzipWriterPool = sync.Pool{
		New: func() interface{} {
			return gzip.NewWriter(nil)
		},
	}

	buffersPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}

	compressors[parquet.CompressionCodec_GZIP] = &Compressor{
		Compress: func(buf []byte) []byte {
			res := buffersPool.Get().(*bytes.Buffer)
			res.Reset()
			gzipWriter := gzipWriterPool.Get().(*gzip.Writer)
			gzipWriter.Reset(res)
			gzipWriter.Write(buf)
			gzipWriter.Close()
			buffersPool.Put(res)
			gzipWriterPool.Put(gzipWriter)
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
