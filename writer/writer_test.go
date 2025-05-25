package writer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source"
	"github.com/hangxie/parquet-go/v2/source/buffer"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
)

// TestNullCountsFromColumnIndex tests that NullCounts is correctly set in the ColumnIndex.
func TestNullCountsFromColumnIndex(t *testing.T) {
	type Entry struct {
		X *int64 `parquet:"name=x, type=INT64"`
		Y *int64 `parquet:"name=y, type=INT64"`
		Z *int64 `parquet:"name=z, type=INT64, omitstats=true"`
		U int64  `parquet:"name=u, type=INT64"`
		V int64  `parquet:"name=v, type=INT64, omitstats=true"`
	}

	type Expect struct {
		IsSetNullCounts bool
		NullCounts      []int64
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, new(Entry), 1)
	assert.NoError(t, err)

	entries := []Entry{
		{val(0), val(0), val(0), 1, 1},
		{nil, val(1), val(1), 2, 2},
		{nil, nil, nil, 3, 3},
	}
	for _, entry := range entries {
		assert.NoError(t, pw.Write(entry))
	}
	assert.NoError(t, pw.WriteStop())

	pf := buffer.NewBufferFileFromBytesNoAlloc(buf.Bytes())
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, nil, 1)
	assert.Nil(t, err)

	assert.Nil(t, pr.ReadFooter())

	assert.Equal(t, 1, len(pr.Footer.RowGroups))
	chunks := pr.Footer.RowGroups[0].GetColumns()
	assert.Equal(t, 5, len(chunks))

	expects := []Expect{
		{true, []int64{2}},
		{true, []int64{1}},
		{false, nil},
		{true, []int64{0}},
		{false, nil},
	}
	for i, chunk := range chunks {
		colIdx, err := readColumnIndex(pr.PFile, *chunk.ColumnIndexOffset)
		assert.NoError(t, err)
		assert.Equal(t, expects[i].IsSetNullCounts, colIdx.IsSetNullCounts())
		assert.Equal(t, expects[i].NullCounts, colIdx.GetNullCounts())
	}
}

// TestAllNullCountsFromColumnIndex tests that NullCounts is correctly set in the ColumnIndex if a field contains null value only.
func TestAllNullCountsFromColumnIndex(t *testing.T) {
	type Entry struct {
		X *int64 `parquet:"name=x, type=INT64"`
		Y *int64 `parquet:"name=z, type=INT64"`
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, new(Entry), 1)
	assert.NoError(t, err)

	entries := []Entry{
		{val(0), nil},
		{val(1), nil},
		{val(2), nil},
		{val(3), nil},
		{val(4), nil},
		{val(5), nil},
	}
	for _, entry := range entries {
		assert.NoError(t, pw.Write(entry))
	}
	assert.NoError(t, pw.WriteStop())

	pf := buffer.NewBufferFileFromBytesNoAlloc(buf.Bytes())
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, nil, 1)
	assert.Nil(t, err)

	assert.Nil(t, pr.ReadFooter())

	assert.Equal(t, 1, len(pr.Footer.RowGroups))
	columns := pr.Footer.RowGroups[0].GetColumns()
	assert.Equal(t, 2, len(columns))

	colIdx, err := readColumnIndex(pr.PFile, *columns[0].ColumnIndexOffset)
	assert.NoError(t, err)
	assert.Equal(t, true, colIdx.IsSetNullCounts())
	assert.Equal(t, []int64{0}, colIdx.GetNullCounts())

	colIdx, err = readColumnIndex(pr.PFile, *columns[1].ColumnIndexOffset)
	assert.NoError(t, err)
	assert.Equal(t, true, colIdx.IsSetNullCounts())
	assert.Equal(t, []int64{6}, colIdx.GetNullCounts())
}

func readColumnIndex(pf source.ParquetFileReader, offset int64) (*parquet.ColumnIndex, error) {
	colIdx := parquet.NewColumnIndex()
	tpf := thrift.NewTCompactProtocolFactoryConf(nil)
	triftReader := source.ConvertToThriftReader(pf, offset)
	protocol := tpf.GetProtocol(triftReader)
	err := colIdx.Read(context.Background(), protocol)
	if err != nil {
		return nil, err
	}
	return colIdx, nil
}

func val(x int64) *int64 {
	y := x
	return &y
}

func TestZeroRows(t *testing.T) {
	type test struct {
		ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	}

	var err error
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	// defer fw.Close()

	// write
	pw, err := NewParquetWriter(fw, new(test), 1)
	assert.NoError(t, err)

	err = pw.WriteStop()
	assert.NoError(t, err)
	assert.NoError(t, fw.Close())

	// read
	pf := buffer.NewBufferFileFromBytesNoAlloc(buf.Bytes())
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, new(test), 1)
	assert.NoError(t, err)

	assert.Equal(t, int64(0), pr.GetNumRows())
}

type test struct {
	ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// TestNullCountsFromColumnIndex tests that NullCounts is correctly set in the ColumnIndex.
func TestDoubleWriteStop(t *testing.T) {
	var err error
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	// defer fw.Close()

	// write
	pw, err := NewParquetWriter(fw, new(test), 1)
	assert.NoError(t, err)

	for i := 0; i < 3; i++ {
		stu := test{
			ColA: fmt.Sprintf("cola_%d", i),
			ColB: fmt.Sprintf("colb_%d", i),
		}
		assert.NoError(t, pw.Write(stu))
	}

	err = pw.WriteStop()
	assert.NoError(t, err)

	err = pw.WriteStop()
	assert.NoError(t, err)

	assert.NoError(t, fw.Close())

	// read
	pf := buffer.NewBufferFileFromBytesNoAlloc(buf.Bytes())
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, new(test), 1)
	assert.NoError(t, err)

	num := int(pr.GetNumRows())
	rows := make([]test, num)
	err = pr.Read(&rows)
	assert.NoError(t, err)

	pr.ReadStop()
}

var errWrite = errors.New("test error")

type invalidFileWriter struct {
	source.ParquetFileWriter
}

func (m *invalidFileWriter) Write(data []byte) (n int, err error) {
	return 0, errWrite
}

func TestNewWriterWithInvaidFile(t *testing.T) {
	pw, err := NewParquetWriter(&invalidFileWriter{}, new(test), 1)
	assert.Nil(t, pw)
	assert.ErrorIs(t, err, errWrite)
}

func TestWriteStopRaceConditionOnError(t *testing.T) {
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewJSONWriter(`{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=x, type=INT64"}]}`, fw, 4)
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		entry := fmt.Sprintf(`{"not-x":%d}`, i)
		assert.NoError(t, pw.Write(entry))
	}
	assert.Error(t, pw.WriteStop())
}
