package reader

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/hangxie/parquet-go/v2/source/buffer"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
	"github.com/hangxie/parquet-go/v2/writer"
	"github.com/stretchr/testify/require"
)

type Record struct {
	Str1 string `parquet:"name=str1, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Str2 string `parquet:"name=str2, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Str3 string `parquet:"name=str3, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Str4 string `parquet:"name=str4, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Int1 int64  `parquet:"name=int1, type=INT64, convertedtype=INT_64, encoding=PLAIN"`
	Int2 int64  `parquet:"name=int2, type=INT64, convertedtype=INT_64, encoding=PLAIN"`
	Int3 int64  `parquet:"name=int3, type=INT64, convertedtype=INT_64, encoding=PLAIN"`
	Int4 int64  `parquet:"name=int4, type=INT64, convertedtype=INT_64, encoding=PLAIN"`
}

var numRecord = int64(500_000)

var parquetBuf []byte

func parquetReader() (*ParquetReader, error) {
	var once sync.Once
	var err error
	once.Do(func() {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		var pw *writer.ParquetWriter
		pw, err = writer.NewParquetWriter(fw, new(Record), 1)
		if err != nil {
			return
		}
		pw.RowGroupSize = 1 * 1024 * 1024 // 1M
		pw.PageSize = 4 * 1024            // 4K
		for i := range numRecord {
			strVal := strconv.FormatInt(i, 10)
			pw.Write(Record{strVal, strVal, strVal, strVal, i, i, i, i})
		}
		if err = pw.WriteStop(); err != nil {
			return
		}
		err = pw.WriteStop()
		parquetBuf = buf.Bytes()
	})
	if err != nil {
		return nil, err
	}
	buf := buffer.NewBufferReaderFromBytesNoAlloc(parquetBuf)
	return NewParquetReader(buf, new(Record), int64(runtime.NumCPU()))
}

func rowsLeft(pr *ParquetReader) (int64, error) {
	result := 0
	for {
		rows, err := pr.ReadByNumber(1000)
		if err != nil {
			return 0, err
		}
		if len(rows) == 0 {
			break
		}
		result += len(rows)
	}
	return int64(result), nil
}

func Test_ParquetReader_SkipRows(t *testing.T) {
	testCases := map[string]struct {
		skip int64
	}{
		"100":    {100},
		"1000":   {1000},
		"10000":  {10000},
		"100000": {100000},
		"max":    {numRecord},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pr, err := parquetReader()
			require.NoError(t, err)
			err = pr.SkipRows(tc.skip)
			require.NoError(t, err)
			num, err := rowsLeft(pr)
			require.NoError(t, err)
			require.Equal(t, numRecord-tc.skip, num)
		})
	}
}

func Benchmark_ParquetReader_SkipRows(b *testing.B) {
	// avoid spending time on data generation
	_, _ = parquetReader()

	for b.Loop() {
		pr, _ := parquetReader()
		_ = pr.SkipRows(numRecord / 2)
	}
}
