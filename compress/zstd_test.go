package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_Codec_ZSTD(t *testing.T) {
	raw := []byte{1, 2, 3}
	compressed := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x4, 0x0, 0x19, 0x0, 0x0, 0x1, 0x2, 0x3, 0xa5, 0xe5, 0x4e, 0xc}

	actual := compressors[parquet.CompressionCodec_ZSTD].Compress(raw)
	require.Equal(t, compressed, actual)

	uncompressed, err := compressors[parquet.CompressionCodec_ZSTD].Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = compressors[parquet.CompressionCodec_ZSTD].Uncompress([]byte{0})
	require.Contains(t, err.Error(), "unexpected EOF")
}
