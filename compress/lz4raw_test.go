package compress

import (
	"bytes"
	"sync"
	"testing"

	"github.com/xitongsys/parquet-go/parquet"
)

func TestLz4RawCompress(t *testing.T) {
	lz4RawCompressor := compressors[parquet.CompressionCodec_LZ4_RAW]
	input := []byte("Peter Parker")
	compressed := []byte{
		0xc0, 0x50, 0x65, 0x74, 0x65, 0x72, 0x20, 0x50, 0x61, 0x72, 0x6b, 0x65, 0x72,
	}

	// compression
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			output := lz4RawCompressor.Compress(input)
			if !bytes.Equal(compressed, output) {
				t.Errorf("expected output %s but was %s", string(compressed), string(output))
			}
		}()
	}
	wg.Wait()

	// uncompression
	output, err := lz4RawCompressor.Uncompress(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(input, output) {
		t.Fatalf("expected output %s but was %s", string(input), string(output))
	}
}
