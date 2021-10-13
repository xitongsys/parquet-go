package writer

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/reader"
)

// testSchema is schema for the testint table which covers all
// the types which we support from the arrow with two exceptions
// which are left commented out
var testSchema = arrow.NewSchema(
	[]arrow.Field{
		{Name: "int8", Type: arrow.PrimitiveTypes.Int8},
		{Name: "int16", Type: arrow.PrimitiveTypes.Int16},
		{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "uint8", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "uint16", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "uint32", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "uint64", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "float32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "float64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "pt-date32", Type: arrow.PrimitiveTypes.Date32},
		{Name: "pt-date64", Type: arrow.PrimitiveTypes.Date64},
		//{Name: "fwt-date32", Type: arrow.FixedWidthTypes.Date32},
		//{Name: "fwt-date64", Type: arrow.FixedWidthTypes.Date64},
		{Name: "bin", Type: arrow.BinaryTypes.Binary},
		{Name: "str", Type: arrow.BinaryTypes.String},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "t32ms", Type: arrow.FixedWidthTypes.Time32ms},
		{Name: "ts-ms", Type: arrow.FixedWidthTypes.Timestamp_ms},
	},
	nil,
)

// testRecord populates the schema testSchema with proper values
func testRecord(mem memory.Allocator) array.Record {
	col1 := func() array.Interface {
		ib := array.NewInt8Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int8{-1, -2, -3, -4, -5, -6, -7, -8, -9, -10}, nil)
		return ib.NewInt8Array()
	}()
	defer col1.Release()
	col2 := func() array.Interface {
		ib := array.NewInt16Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int16{-11, -12, -13, -14, -15, -16, -17, -18, -19,
			-20}, nil)
		return ib.NewInt16Array()
	}()
	defer col2.Release()
	col3 := func() array.Interface {
		ib := array.NewInt32Builder(mem)
		defer ib.Release()
		ib.AppendValues([]int32{-21, -22, -23, -24, -25, -26, -27, -28, -29,
			-30}, nil)
		return ib.NewInt32Array()
	}()
	defer col3.Release()
	col4 := func() array.Interface {
		ib := array.NewInt64Builder(mem)
		defer ib.Release()
		ib.AppendValues([]int64{-31, -32, -33, -34, -35, -36, -37, -38, -39,
			-40}, nil)
		return ib.NewInt64Array()
	}()
	defer col4.Release()
	col5 := func() array.Interface {
		ib := array.NewUint8Builder(mem)
		defer ib.Release()
		ib.AppendValues([]uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewUint8Array()
	}()
	defer col5.Release()
	col6 := func() array.Interface {
		ib := array.NewUint16Builder(mem)
		defer ib.Release()
		ib.AppendValues([]uint16{11, 12, 13, 14, 15, 16, 17, 18, 19,
			20}, nil)
		return ib.NewUint16Array()
	}()
	defer col6.Release()
	col7 := func() array.Interface {
		ib := array.NewUint32Builder(mem)
		defer ib.Release()
		ib.AppendValues([]uint32{21, 22, 23, 24, 25, 26, 27, 28, 29,
			30}, nil)
		return ib.NewUint32Array()
	}()
	defer col7.Release()
	col8 := func() array.Interface {
		ib := array.NewUint64Builder(mem)
		defer ib.Release()
		ib.AppendValues([]uint64{31, 32, 33, 34, 35, 36, 37, 38, 39,
			40}, nil)
		return ib.NewUint64Array()
	}()
	defer col8.Release()
	col9 := func() array.Interface {
		ib := array.NewFloat32Builder(mem)
		defer ib.Release()
		ib.AppendValues([]float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7,
			8.8, 9.9, 10.10}, nil)
		return ib.NewFloat32Array()
	}()
	defer col9.Release()
	col10 := func() array.Interface {
		ib := array.NewFloat64Builder(mem)
		defer ib.Release()
		ib.AppendValues([]float64{10.1, 12.2, 13.3, 14.4, 15.5, 16.6,
			17.7, 18.8, 19.9, 20.10}, nil)
		return ib.NewFloat64Array()
	}()
	defer col10.Release()
	col11 := func() array.Interface {
		ib := array.NewDate32Builder(mem)
		defer ib.Release()
		ib.AppendValues([]arrow.Date32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nil)
		return ib.NewDate32Array()
	}()
	defer col11.Release()
	col12 := func() array.Interface {
		ib := array.NewDate64Builder(mem)
		defer ib.Release()
		ib.AppendValues([]arrow.Date64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nil)
		return ib.NewDate64Array()
	}()
	defer col12.Release()
	col13 := func() array.Interface {
		ib := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer ib.Release()
		ib.AppendValues([][]byte{[]byte("A"), []byte("B"), []byte("C"),
			[]byte("D"), []byte("E"), []byte("F"), []byte("G"),
			[]byte("H"), []byte("I"), []byte("J")}, nil)
		return ib.NewBinaryArray()
	}()
	defer col13.Release()
	col14 := func() array.Interface {
		ib := array.NewStringBuilder(mem)
		defer ib.Release()
		ib.AppendValues([]string{"a", "b", "c", "d", "e", "f", "g",
			"h", "i", "j"}, nil)
		return ib.NewStringArray()
	}()
	defer col14.Release()
	col15 := func() array.Interface {
		ib := array.NewBooleanBuilder(mem)
		defer ib.Release()
		ib.AppendValues([]bool{true, false, true, false, true,
			false, true, false, true, false}, nil)
		return ib.NewBooleanArray()
	}()
	defer col15.Release()
	col16 := func() array.Interface {
		dtype := arrow.FixedWidthTypes.Time32ms
		ib := array.NewTime32Builder(mem, dtype.(*arrow.Time32Type))
		defer ib.Release()
		ib.AppendValues([]arrow.Time32{arrow.Time32(1), arrow.Time32(2),
			arrow.Time32(3), arrow.Time32(4), arrow.Time32(5),
			arrow.Time32(6), arrow.Time32(7), arrow.Time32(8),
			arrow.Time32(9), arrow.Time32(10)}, nil)
		return ib.NewTime32Array()
	}()
	defer col16.Release()
	col17 := func() array.Interface {
		dtype := arrow.FixedWidthTypes.Timestamp_ms
		ib := array.NewTimestampBuilder(mem, dtype.(*arrow.TimestampType))
		defer ib.Release()
		ib.AppendValues([]arrow.Timestamp{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nil)
		return ib.NewTimestampArray()
	}()
	defer col17.Release()
	cols := []array.Interface{col1, col2, col3, col4, col5, col6, col7,
		col8, col9, col10, col11, col12, col13, col14, col15, col16, col17}
	return array.NewRecord(testSchema, cols, -1)
}

// TestE2EValid tests the whole cycle of creating a parquet file from arrow
// covering all the currently supported types by using sequential writer
// runing a single goroutine.
// This test does not go through FixedWidthTypes of Date 32 and 64 as there
// was no convenient way to mock this and the PrimitiveTypes of those are
// used instead
func TestE2ESequentialValid(t *testing.T) {
	var err error
	ts := testSchema

	buf := new(bytes.Buffer)
	fw := writerfile.NewWriterFile(buf)
	assert.Nil(t, err)

	w, err := NewArrowWriter(ts, fw, 1)
	assert.Nil(t, err)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rec := testRecord(mem)
	defer rec.Release()
	err = w.WriteArrow(rec)
	assert.Nil(t, err)

	err = w.WriteStop()
	assert.Nil(t, err)

	parquetFile, err := buffer.NewBufferFile(buf.Bytes())
	assert.Nil(t, err)

	pr, err := reader.NewParquetReader(parquetFile, nil, 1)
	assert.Nil(t, err)

	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	assert.Nil(t, err)

	actualTable := ""
	for _, row := range res {
		actualTable = actualTable + fmt.Sprintf("%v", row)
	}
	expectedTable := "" +
		"{-1 -11 -21 -31 1 11 21 31 1.1 10.1 1 1 [65] a true 1 1}" +
		"{-2 -12 -22 -32 2 12 22 32 2.2 12.2 2 2 [66] b false 2 2}" +
		"{-3 -13 -23 -33 3 13 23 33 3.3 13.3 3 3 [67] c true 3 3}" +
		"{-4 -14 -24 -34 4 14 24 34 4.4 14.4 4 4 [68] d false 4 4}" +
		"{-5 -15 -25 -35 5 15 25 35 5.5 15.5 5 5 [69] e true 5 5}" +
		"{-6 -16 -26 -36 6 16 26 36 6.6 16.6 6 6 [70] f false 6 6}" +
		"{-7 -17 -27 -37 7 17 27 37 7.7 17.7 7 7 [71] g true 7 7}" +
		"{-8 -18 -28 -38 8 18 28 38 8.8 18.8 8 8 [72] h false 8 8}" +
		"{-9 -19 -29 -39 9 19 29 39 9.9 19.9 9 9 [73] i true 9 9}" +
		"{-10 -20 -30 -40 10 20 30 40 10.1 20.1 10 10 [74] j false 10 10}"
	assert.Equal(t, expectedTable, actualTable)

	err = fw.Close()
	assert.Nil(t, err)
	pr.ReadStop()
	err = parquetFile.Close()
	assert.Nil(t, err)
}

// TestE2EConcurrentValid tests the whole cycle of creating a parquet file
// from arrow covering all the currently supported types by using a
// concurrent writer running four goroutines
//
// This test does not go through FixedWidthTypes of Date 32 and 64 as there
// was no convenient way to mock this and the PrimitiveTypes of those are
// used instead
func TestE2EConcurrentValid(t *testing.T) {
	var err error
	ts := testSchema

	buf := new(bytes.Buffer)
	fw := writerfile.NewWriterFile(buf)
	assert.Nil(t, err)

	w, err := NewArrowWriter(ts, fw, 4)
	assert.Nil(t, err)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rec := testRecord(mem)
	defer rec.Release()
	err = w.WriteArrow(rec)
	assert.Nil(t, err)

	err = w.WriteStop()
	assert.Nil(t, err)

	parquetFile, err := buffer.NewBufferFile(buf.Bytes())
	assert.Nil(t, err)

	pr, err := reader.NewParquetReader(parquetFile, nil, 1)
	assert.Nil(t, err)

	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	assert.Nil(t, err)

	actualTable := ""
	for _, row := range res {
		actualTable = actualTable + fmt.Sprintf("%v", row)
	}
	expectedTable := "" +
		"{-1 -11 -21 -31 1 11 21 31 1.1 10.1 1 1 [65] a true 1 1}" +
		"{-2 -12 -22 -32 2 12 22 32 2.2 12.2 2 2 [66] b false 2 2}" +
		"{-3 -13 -23 -33 3 13 23 33 3.3 13.3 3 3 [67] c true 3 3}" +
		"{-4 -14 -24 -34 4 14 24 34 4.4 14.4 4 4 [68] d false 4 4}" +
		"{-5 -15 -25 -35 5 15 25 35 5.5 15.5 5 5 [69] e true 5 5}" +
		"{-6 -16 -26 -36 6 16 26 36 6.6 16.6 6 6 [70] f false 6 6}" +
		"{-7 -17 -27 -37 7 17 27 37 7.7 17.7 7 7 [71] g true 7 7}" +
		"{-8 -18 -28 -38 8 18 28 38 8.8 18.8 8 8 [72] h false 8 8}" +
		"{-9 -19 -29 -39 9 19 29 39 9.9 19.9 9 9 [73] i true 9 9}" +
		"{-10 -20 -30 -40 10 20 30 40 10.1 20.1 10 10 [74] j false 10 10}"
	assert.Equal(t, expectedTable, actualTable)

	err = fw.Close()
	assert.Nil(t, err)
	pr.ReadStop()
	err = parquetFile.Close()
	assert.Nil(t, err)
}

func BenchmarkWrite(b *testing.B) {
	buf := new(bytes.Buffer)
	fw := writerfile.NewWriterFile(buf)
	md := testSchema
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rec := testRecord(mem)
	defer rec.Release()
	pw, _ := NewArrowWriter(md, fw, 1)
	b.ResetTimer()
	for N := 0; N < b.N; N++ {
		_ = pw.WriteArrow(rec)
	}
}
