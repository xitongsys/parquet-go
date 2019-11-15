// +build !no_zstd

package compress

import (
	"github.com/DataDog/zstd"
	"github.com/xitongsys/parquet-go/parquet"
)

func init() {
	compressors[parquet.CompressionCodec_ZSTD] = &Compressor{
		Compress: func(buf []byte) []byte {
			res, _ := zstd.Compress(nil, buf)
			return res
		},
		Uncompress: func(buf []byte) (bytes []byte, err error) {
			return zstd.Decompress(nil, buf)
		},
	}
}
