//go:build !no_gzip
// +build !no_gzip

package compress

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/compress/gzip"

	"github.com/hangxie/parquet-go/parquet"
)

var gzipWriterPool sync.Pool

func init() {
	gzipWriterPool = sync.Pool{
		New: func() interface{} {
			return gzip.NewWriter(nil)
		},
	}

	compressors[parquet.CompressionCodec_GZIP] = &Compressor{
		Compress: func(buf []byte) []byte {
			res := new(bytes.Buffer)
			gzipWriter := gzipWriterPool.Get().(*gzip.Writer)
			gzipWriter.Reset(res)
			if _, err := gzipWriter.Write(buf); err != nil {
				return nil
			}
			_ = gzipWriter.Close()
			gzipWriter.Reset(nil)
			gzipWriterPool.Put(gzipWriter)
			return res.Bytes()
		},
		Uncompress: func(buf []byte) (i []byte, err error) {
			rbuf := bytes.NewReader(buf)
			gzipReader, _ := gzip.NewReader(rbuf)
			res, err := io.ReadAll(gzipReader)
			return res, err
		},
	}
}
