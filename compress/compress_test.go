package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_Uncompress(t *testing.T) {
	testCases := map[string]struct {
		codec      parquet.CompressionCodec
		compressed []byte
		expected   []byte
		errMsg     string
	}{
		"good":      {parquet.CompressionCodec_SNAPPY, []byte{3, 8, 1, 2, 3}, []byte{1, 2, 3}, ""},
		"bad-input": {parquet.CompressionCodec_SNAPPY, []byte{1, 2, 3}, nil, "corrupt input"},
		"bad-codec": {parquet.CompressionCodec(-1), []byte{1, 2, 3}, nil, "unsupported compress method"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			res, err := Uncompress(tc.compressed, tc.codec)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, res)
			} else if err == nil || tc.errMsg == "" {
				require.EqualError(t, err, tc.errMsg)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_Compress(t *testing.T) {
	testCases := map[string]struct {
		codec    parquet.CompressionCodec
		raw      []byte
		expected []byte
	}{}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, Compress(tc.raw, tc.codec))
		})
	}
}
