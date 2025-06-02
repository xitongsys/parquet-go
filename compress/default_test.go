package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_Codec_UNCOMPRESSED(t *testing.T) {
	raw := []byte{1, 2, 3}
	compressed := compressors[parquet.CompressionCodec_UNCOMPRESSED].Compress(raw)
	require.Equal(t, raw, compressed)

	uncompressed, err := compressors[parquet.CompressionCodec_UNCOMPRESSED].Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)
}
