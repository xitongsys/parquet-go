package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_Codec_LZ4(t *testing.T) {
	raw := []byte{1, 2, 3}
	compressed := []byte{0x4, 0x22, 0x4d, 0x18, 0x64, 0x70, 0xb9, 0x3, 0x0, 0x0, 0x80, 0x1, 0x2, 0x3, 0x0, 0x0, 0x0, 0x0, 0xc4, 0x78, 0x9c, 0xf5}

	actual := compressors[parquet.CompressionCodec_LZ4].Compress(raw)
	require.Equal(t, compressed, actual)

	uncompressed, err := compressors[parquet.CompressionCodec_LZ4].Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = compressors[parquet.CompressionCodec_LZ4].Uncompress([]byte{0})
	require.Contains(t, err.Error(), "unexpected EOF")
}
