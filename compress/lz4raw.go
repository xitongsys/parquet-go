//go:build !no_lz4raw
// +build !no_lz4raw

package compress

import (
	"github.com/pierrec/lz4/v4"

	"github.com/xitongsys/parquet-go/parquet"
)

func init() {
	compressors[parquet.CompressionCodec_LZ4_RAW] = &Compressor{
		Compress: func(buf []byte) []byte {
			lz4hc := lz4.CompressorHC{
				Level: lz4.CompressionLevel(9),
			}
			res := make([]byte, lz4.CompressBlockBound(len(buf)))
			count, _ := lz4hc.CompressBlock(buf, res)
			return res[:count]
		},
		Uncompress: func(buf []byte) (i []byte, err error) {
			res := make([]byte, 255*len(buf))
			count, err := lz4.UncompressBlock(buf, res)
			res = res[:count]
			return res[:count], err
		},
	}
}
