package writer

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/buffer"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
)

func Test_NewCSVWriterFromWriter(t *testing.T) {
	schema := []string{
		"Name=First, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
		"Name=Last, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
	}
	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)
	cw, err := NewCSVWriterFromWriter(schema, bw, 4)
	require.NoError(t, err)
	require.Equal(t, cw.NP, int64(4))
	require.Equal(t, cw.PageSize, int64(8*1024))
	require.Equal(t, cw.RowGroupSize, int64(128*1024*1024))
	require.Equal(t, cw.CompressionType, parquet.CompressionCodec_SNAPPY)
}

func Test_NewCSVWriter(t *testing.T) {
	testCases := map[string]struct {
		schema []string
		errMsg string
	}{
		"bad":   {[]string{"abc"}, "failed to create schema from metadata"},
		"empty": {[]string{}, ""},
		"good":  {[]string{"Name=First, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN"}, ""},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			bw := bufio.NewWriter(&buf)
			wf := writerfile.NewWriterFile(bw)
			cw, err := NewCSVWriter(tc.schema, wf, 4)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, cw.NP, int64(4))
				require.Equal(t, cw.PageSize, int64(8*1024))
				require.Equal(t, cw.RowGroupSize, int64(128*1024*1024))
				require.Equal(t, cw.CompressionType, parquet.CompressionCodec_SNAPPY)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_WriteCSV(t *testing.T) {
	testCases := map[string]struct {
		data   []*string
		errMsg string
	}{
		"empty": {[]*string{nil, nil}, ""},
		"good":  {[]*string{common.ToPtr("name"), common.ToPtr("123")}, ""},
		"bad":   {[]*string{common.ToPtr("name"), common.ToPtr("abc")}, "expected integer"},
	}
	schema := []string{
		"Name=Name, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
		"Name=id, Type=INT32",
	}
	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)
	cw, _ := NewCSVWriterFromWriter(schema, bw, 4)

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := cw.WriteString(tc.data)
			if err == nil && tc.errMsg == "" {
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Benchmark_WriteCSV(b *testing.B) {
	b.ReportAllocs()
	md := []string{
		"Name=First, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN_DICTIONARY",
		"Name=Middle, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN_DICTIONARY",
		"Name=Last, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN_DICTIONARY",
		"Name=BirthCity, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN_DICTIONARY",
	}
	for b.Loop() {
		fw := buffer.NewBufferWriterFromBytesNoAlloc(nil)
		pw, err := NewCSVWriter(md, fw, 2)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 10000; j++ {
			err = pw.Write([]any{"Harry", "S", "Truman", "Lamar"})
			if err != nil {
				b.Fatal(err)
			}
		}
		err = pw.WriteStop()
		if err != nil {
			b.Fatal(err)
		}
		_ = fw.Close()
	}
}

func Benchmark_WriteCSVPlainDictionary(b *testing.B) {
	b.ReportAllocs()
	md := []string{
		"Name=First, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN_DICTIONARY",
		"Name=Middle, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN_DICTIONARY",
		"Name=Last, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN_DICTIONARY",
		"Name=BirthCity, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN_DICTIONARY",
	}
	for i := 0; i < b.N; i++ {
		fw := buffer.NewBufferWriterFromBytesNoAlloc(nil)
		pw, err := NewCSVWriter(md, fw, 2)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 10000; j++ {
			err = pw.Write([]interface{}{"Harry", "S", "Truman", "Lamar"})
			if err != nil {
				b.Fatal(err)
			}
		}
		err = pw.WriteStop()
		if err != nil {
			b.Fatal(err)
		}
		_ = fw.Close()
	}
}
