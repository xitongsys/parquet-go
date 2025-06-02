package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_Codec_SNAPPY(t *testing.T) {
	raw := []byte{1, 2, 3}
	compressed := []byte{0x3, 0x8, 0x1, 0x2, 0x3}

	actual := compressors[parquet.CompressionCodec_SNAPPY].Compress(raw)
	require.Equal(t, compressed, actual)

	uncompressed, err := compressors[parquet.CompressionCodec_SNAPPY].Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = compressors[parquet.CompressionCodec_SNAPPY].Uncompress([]byte{1})
	require.Contains(t, err.Error(), "corrupt input")
}
