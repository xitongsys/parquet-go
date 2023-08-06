package writer

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/reader"
)

// testSchema is schema for the testint table which covers all
// the types which we support from the arrow.
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
		{Name: "bin", Type: arrow.BinaryTypes.Binary},
		{Name: "str", Type: arrow.BinaryTypes.String},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "fwt-date32", Type: arrow.FixedWidthTypes.Date32},
		{Name: "fwt-date64", Type: arrow.FixedWidthTypes.Date64},
		{Name: "t32ms", Type: arrow.FixedWidthTypes.Time32ms},
		{Name: "ts-ms", Type: arrow.FixedWidthTypes.Timestamp_ms},
	},
	nil,
)

// testRecord populates the schema testSchema with proper values
func testRecord(mem memory.Allocator) arrow.Record {
	col1 := func() arrow.Array {
		ib := array.NewInt8Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int8{-1, -2, -3, -4, -5, -6, -7, -8, -9, -10}, nil)
		return ib.NewInt8Array()
	}()
	defer col1.Release()
	col2 := func() arrow.Array {
		ib := array.NewInt16Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int16{-11, -12, -13, -14, -15, -16, -17, -18, -19,
			-20}, nil)
		return ib.NewInt16Array()
	}()
	defer col2.Release()
	col3 := func() arrow.Array {
		ib := array.NewInt32Builder(mem)
		defer ib.Release()
		ib.AppendValues([]int32{-21, -22, -23, -24, -25, -26, -27, -28, -29,
			-30}, nil)
		return ib.NewInt32Array()
	}()
	defer col3.Release()
	col4 := func() arrow.Array {
		ib := array.NewInt64Builder(mem)
		defer ib.Release()
		ib.AppendValues([]int64{-31, -32, -33, -34, -35, -36, -37, -38, -39,
			-40}, nil)
		return ib.NewInt64Array()
	}()
	defer col4.Release()
	col5 := func() arrow.Array {
		ib := array.NewUint8Builder(mem)
		defer ib.Release()
		ib.AppendValues([]uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewUint8Array()
	}()
	defer col5.Release()
	col6 := func() arrow.Array {
		ib := array.NewUint16Builder(mem)
		defer ib.Release()
		ib.AppendValues([]uint16{11, 12, 13, 14, 15, 16, 17, 18, 19,
			20}, nil)
		return ib.NewUint16Array()
	}()
	defer col6.Release()
	col7 := func() arrow.Array {
		ib := array.NewUint32Builder(mem)
		defer ib.Release()
		ib.AppendValues([]uint32{21, 22, 23, 24, 25, 26, 27, 28, 29,
			30}, nil)
		return ib.NewUint32Array()
	}()
	defer col7.Release()
	col8 := func() arrow.Array {
		ib := array.NewUint64Builder(mem)
		defer ib.Release()
		ib.AppendValues([]uint64{31, 32, 33, 34, 35, 36, 37, 38, 39,
			40}, nil)
		return ib.NewUint64Array()
	}()
	defer col8.Release()
	col9 := func() arrow.Array {
		ib := array.NewFloat32Builder(mem)
		defer ib.Release()
		ib.AppendValues([]float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7,
			8.8, 9.9, 10.10}, nil)
		return ib.NewFloat32Array()
	}()
	defer col9.Release()
	col10 := func() arrow.Array {
		ib := array.NewFloat64Builder(mem)
		defer ib.Release()
		ib.AppendValues([]float64{10.1, 12.2, 13.3, 14.4, 15.5, 16.6,
			17.7, 18.8, 19.9, 20.10}, nil)
		return ib.NewFloat64Array()
	}()
	defer col10.Release()
	col11 := func() arrow.Array {
		ib := array.NewDate32Builder(mem)
		defer ib.Release()
		ib.AppendValues([]arrow.Date32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nil)
		return ib.NewDate32Array()
	}()
	defer col11.Release()
	col12 := func() arrow.Array {
		ib := array.NewDate64Builder(mem)
		defer ib.Release()
		ib.AppendValues([]arrow.Date64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nil)
		return ib.NewDate64Array()
	}()
	defer col12.Release()
	col13 := func() arrow.Array {
		ib := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer ib.Release()
		ib.AppendValues([][]byte{[]byte("A"), []byte("B"), []byte("C"),
			[]byte("D"), []byte("E"), []byte("F"), []byte("G"),
			[]byte("H"), []byte("I"), []byte("J")}, nil)
		return ib.NewBinaryArray()
	}()
	defer col13.Release()
	col14 := func() arrow.Array {
		ib := array.NewStringBuilder(mem)
		defer ib.Release()
		ib.AppendValues([]string{"a", "b", "c", "d", "e", "f", "g",
			"h", "i", "j"}, nil)
		return ib.NewStringArray()
	}()
	defer col14.Release()
	col15 := func() arrow.Array {
		ib := array.NewBooleanBuilder(mem)
		defer ib.Release()
		ib.AppendValues([]bool{true, false, true, false, true,
			false, true, false, true, false}, nil)
		return ib.NewBooleanArray()
	}()
	defer col15.Release()
	col16 := func() arrow.Array {
		ib := array.NewDate32Builder(mem)
		defer ib.Release()
		ib.AppendValues([]arrow.Date32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nil)
		return ib.NewDate32Array()
	}()
	defer col16.Release()
	col17 := func() arrow.Array {
		ib := array.NewDate64Builder(mem)
		defer ib.Release()
		ib.AppendValues([]arrow.Date64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nil)
		return ib.NewDate64Array()
	}()
	defer col17.Release()
	col18 := func() arrow.Array {
		dtype := arrow.FixedWidthTypes.Time32ms
		ib := array.NewTime32Builder(mem, dtype.(*arrow.Time32Type))
		defer ib.Release()
		ib.AppendValues([]arrow.Time32{arrow.Time32(1), arrow.Time32(2),
			arrow.Time32(3), arrow.Time32(4), arrow.Time32(5),
			arrow.Time32(6), arrow.Time32(7), arrow.Time32(8),
			arrow.Time32(9), arrow.Time32(10)}, nil)
		return ib.NewTime32Array()
	}()
	defer col18.Release()
	col19 := func() arrow.Array {
		dtype := arrow.FixedWidthTypes.Timestamp_ms
		ib := array.NewTimestampBuilder(mem, dtype.(*arrow.TimestampType))
		defer ib.Release()
		ib.AppendValues([]arrow.Timestamp{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nil)
		return ib.NewTimestampArray()
	}()
	defer col19.Release()
	cols := []arrow.Array{col1, col2, col3, col4, col5, col6, col7,
		col8, col9, col10, col11, col12, col13, col14, col15, col16, col17,
		col18, col19}
	return array.NewRecord(testSchema, cols, -1)
}

// testNullableSchema is schema for the testing the support for nullability
// and covers all the types which we support from the arrow.
var testNullableSchema = arrow.NewSchema(
	[]arrow.Field{
		{Name: "int8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "int16", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "int32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "int64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "uint8", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
		{Name: "uint16", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
		{Name: "uint32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "uint64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "float32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "float64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "pt-date32", Type: arrow.PrimitiveTypes.Date32, Nullable: true},
		{Name: "pt-date64", Type: arrow.PrimitiveTypes.Date64, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "fwt-date32", Type: arrow.FixedWidthTypes.Date32,
			Nullable: true},
		{Name: "fwt-date64", Type: arrow.FixedWidthTypes.Date64,
			Nullable: true},
		{Name: "t32ms", Type: arrow.FixedWidthTypes.Time32ms, Nullable: true},
		{Name: "ts-ms", Type: arrow.FixedWidthTypes.Timestamp_ms,
			Nullable: true},
	},
	nil,
)

// testRecordWithNulls populates the schema testNullableSchema
func testRecordWithNulls(mem memory.Allocator) arrow.Record {
	col1 := func() arrow.Array {
		ib := array.NewInt8Builder(mem)
		defer ib.Release()
		ib.Append(-1)
		ib.AppendNull()
		ib.Append(-2)
		ib.AppendNull()
		return ib.NewInt8Array()
	}()
	defer col1.Release()
	col2 := func() arrow.Array {
		ib := array.NewInt16Builder(mem)
		defer ib.Release()
		ib.AppendNull()
		ib.Append(-11)
		ib.AppendNull()
		ib.Append(-12)
		return ib.NewInt16Array()
	}()
	defer col2.Release()
	col3 := func() arrow.Array {
		ib := array.NewInt32Builder(mem)
		defer ib.Release()
		ib.Append(-21)
		ib.AppendNull()
		ib.Append(-22)
		ib.AppendNull()
		return ib.NewInt32Array()
	}()
	defer col3.Release()
	col4 := func() arrow.Array {
		ib := array.NewInt64Builder(mem)
		defer ib.Release()
		ib.AppendNull()
		ib.Append(-31)
		ib.AppendNull()
		ib.Append(-32)
		return ib.NewInt64Array()
	}()
	defer col4.Release()
	col5 := func() arrow.Array {
		ib := array.NewUint8Builder(mem)
		defer ib.Release()
		ib.Append(1)
		ib.AppendNull()
		ib.Append(2)
		ib.AppendNull()
		return ib.NewUint8Array()
	}()
	defer col5.Release()
	col6 := func() arrow.Array {
		ib := array.NewUint16Builder(mem)
		defer ib.Release()
		ib.AppendNull()
		ib.Append(11)
		ib.AppendNull()
		ib.Append(12)
		return ib.NewUint16Array()
	}()
	defer col6.Release()
	col7 := func() arrow.Array {
		ib := array.NewUint32Builder(mem)
		defer ib.Release()
		ib.Append(21)
		ib.AppendNull()
		ib.Append(22)
		ib.AppendNull()
		return ib.NewUint32Array()
	}()
	defer col7.Release()
	col8 := func() arrow.Array {
		ib := array.NewUint64Builder(mem)
		defer ib.Release()
		ib.AppendNull()
		ib.Append(31)
		ib.AppendNull()
		ib.Append(32)
		return ib.NewUint64Array()
	}()
	defer col8.Release()
	col9 := func() arrow.Array {
		ib := array.NewFloat32Builder(mem)
		defer ib.Release()
		ib.Append(1.1)
		ib.AppendNull()
		ib.Append(1.2)
		ib.AppendNull()
		return ib.NewFloat32Array()
	}()
	defer col9.Release()
	col10 := func() arrow.Array {
		ib := array.NewFloat64Builder(mem)
		defer ib.Release()
		ib.AppendNull()
		ib.Append(10.1)
		ib.AppendNull()
		ib.Append(10.2)
		return ib.NewFloat64Array()
	}()
	defer col10.Release()
	col11 := func() arrow.Array {
		ib := array.NewDate32Builder(mem)
		defer ib.Release()
		ib.Append(1)
		ib.AppendNull()
		ib.Append(2)
		ib.AppendNull()
		return ib.NewDate32Array()
	}()
	defer col11.Release()
	col12 := func() arrow.Array {
		ib := array.NewDate64Builder(mem)
		defer ib.Release()
		ib.AppendNull()
		ib.Append(1)
		ib.AppendNull()
		ib.Append(2)
		return ib.NewDate64Array()
	}()
	defer col12.Release()
	col13 := func() arrow.Array {
		ib := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer ib.Release()
		ib.Append([]byte("A"))
		ib.AppendNull()
		ib.Append([]byte("B"))
		ib.AppendNull()
		return ib.NewBinaryArray()
	}()
	defer col13.Release()
	col14 := func() arrow.Array {
		ib := array.NewStringBuilder(mem)
		defer ib.Release()
		ib.AppendNull()
		ib.Append("a")
		ib.AppendNull()
		ib.Append("b")
		return ib.NewStringArray()
	}()
	defer col14.Release()
	col15 := func() arrow.Array {
		ib := array.NewBooleanBuilder(mem)
		defer ib.Release()
		ib.Append(true)
		ib.AppendNull()
		ib.Append(false)
		ib.AppendNull()
		return ib.NewBooleanArray()
	}()
	defer col15.Release()
	col16 := func() arrow.Array {
		ib := array.NewDate32Builder(mem)
		defer ib.Release()
		ib.AppendNull()
		ib.Append(1)
		ib.AppendNull()
		ib.Append(2)
		return ib.NewDate32Array()
	}()
	defer col16.Release()
	col17 := func() arrow.Array {
		ib := array.NewDate64Builder(mem)
		defer ib.Release()
		ib.Append(1)
		ib.AppendNull()
		ib.Append(2)
		ib.AppendNull()
		return ib.NewDate64Array()
	}()
	defer col17.Release()
	col18 := func() arrow.Array {
		dtype := arrow.FixedWidthTypes.Time32ms
		ib := array.NewTime32Builder(mem, dtype.(*arrow.Time32Type))
		defer ib.Release()
		ib.AppendNull()
		ib.Append(1)
		ib.AppendNull()
		ib.Append(2)
		return ib.NewTime32Array()
	}()
	defer col18.Release()
	col19 := func() arrow.Array {
		dtype := arrow.FixedWidthTypes.Timestamp_ms
		ib := array.NewTimestampBuilder(mem, dtype.(*arrow.TimestampType))
		defer ib.Release()
		ib.Append(1)
		ib.AppendNull()
		ib.Append(2)
		ib.AppendNull()
		return ib.NewTimestampArray()
	}()
	defer col19.Release()
	cols := []arrow.Array{col1, col2, col3, col4, col5, col6, col7,
		col8, col9, col10, col11, col12, col13, col14, col15, col16, col17,
		col18, col19}
	return array.NewRecord(testNullableSchema, cols, -1)
}

// TestE2EValid tests the whole cycle of creating a parquet file from arrow
// covering all the currently supported types by using sequential writer
// running a single goroutine.
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
		"{-1 -11 -21 -31 1 11 21 31 1.1 10.1 1 1 A a true 1 1 1 1}" +
		"{-2 -12 -22 -32 2 12 22 32 2.2 12.2 2 2 B b false 2 2 2 2}" +
		"{-3 -13 -23 -33 3 13 23 33 3.3 13.3 3 3 C c true 3 3 3 3}" +
		"{-4 -14 -24 -34 4 14 24 34 4.4 14.4 4 4 D d false 4 4 4 4}" +
		"{-5 -15 -25 -35 5 15 25 35 5.5 15.5 5 5 E e true 5 5 5 5}" +
		"{-6 -16 -26 -36 6 16 26 36 6.6 16.6 6 6 F f false 6 6 6 6}" +
		"{-7 -17 -27 -37 7 17 27 37 7.7 17.7 7 7 G g true 7 7 7 7}" +
		"{-8 -18 -28 -38 8 18 28 38 8.8 18.8 8 8 H h false 8 8 8 8}" +
		"{-9 -19 -29 -39 9 19 29 39 9.9 19.9 9 9 I i true 9 9 9 9}" +
		"{-10 -20 -30 -40 10 20 30 40 10.1 20.1 10 10 J j false 10 10 10 10}"
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
		"{-1 -11 -21 -31 1 11 21 31 1.1 10.1 1 1 A a true 1 1 1 1}" +
		"{-2 -12 -22 -32 2 12 22 32 2.2 12.2 2 2 B b false 2 2 2 2}" +
		"{-3 -13 -23 -33 3 13 23 33 3.3 13.3 3 3 C c true 3 3 3 3}" +
		"{-4 -14 -24 -34 4 14 24 34 4.4 14.4 4 4 D d false 4 4 4 4}" +
		"{-5 -15 -25 -35 5 15 25 35 5.5 15.5 5 5 E e true 5 5 5 5}" +
		"{-6 -16 -26 -36 6 16 26 36 6.6 16.6 6 6 F f false 6 6 6 6}" +
		"{-7 -17 -27 -37 7 17 27 37 7.7 17.7 7 7 G g true 7 7 7 7}" +
		"{-8 -18 -28 -38 8 18 28 38 8.8 18.8 8 8 H h false 8 8 8 8}" +
		"{-9 -19 -29 -39 9 19 29 39 9.9 19.9 9 9 I i true 9 9 9 9}" +
		"{-10 -20 -30 -40 10 20 30 40 10.1 20.1 10 10 J j false 10 10 10 10}"
	assert.Equal(t, expectedTable, actualTable)

	err = fw.Close()
	assert.Nil(t, err)
	pr.ReadStop()
	err = parquetFile.Close()
	assert.Nil(t, err)
}

// TestE2NullabilityValid tests the whole cycle of creating a parquet file
// from arrow record which contains Null values covering all the currently
// supported types by using schema in which all fields are marked Nullable.
func TestE2ENullabilityValid(t *testing.T) {
	var err error
	ts := testNullableSchema

	buf := new(bytes.Buffer)
	fw := writerfile.NewWriterFile(buf)
	assert.Nil(t, err)

	w, err := NewArrowWriter(ts, fw, 1)
	assert.Nil(t, err)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rec := testRecordWithNulls(mem)
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

	actualTable := [][]interface{}{}
	for _, row := range res {
		actualTable = append(actualTable, rowToSliceOfValues(row))
	}
	expectedTable := [][]interface{}{
		{-1, nil, -21, nil, 1, nil, 21, nil, 1.1, nil, 1, nil, "A", nil, true,
			nil, 1, nil, 1},

		{nil, -11, nil, -31, nil, 11, nil, 31, nil, 10.1, nil, 1, nil, "a",
			nil, 1, nil, 1, nil},

		{-2, nil, -22, nil, 2, nil, 22, nil, 1.2, nil, 2, nil, "B", nil, false,
			nil, 2, nil, 2},

		{nil, -12, nil, -32, nil, 12, nil, 32, nil, 10.2, nil, 2, nil, "b",
			nil, 2, nil, 2, nil},
	}
	assert.Equal(t, len(expectedTable), len(actualTable))
	assert.Equal(t, len(expectedTable[0]), len(actualTable[0]))
	for i := range expectedTable {
		for j := range expectedTable[i] {
			assert.EqualValues(t, expectedTable[i][j], actualTable[i][j],
				"mismatch at: [%d][%d]", i, j)
		}
	}

	err = fw.Close()
	assert.Nil(t, err)
	pr.ReadStop()
	err = parquetFile.Close()
	assert.Nil(t, err)
}

func rowToSliceOfValues(s interface{}) []interface{} {
	v := reflect.ValueOf(s)
	res := []interface{}{}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.IsNil() {
			res = append(res, nil)
			continue
		}
		if field.Type().Kind() == reflect.Ptr {
			res = append(res, field.Elem().Interface())
		} else {
			res = append(res, field.Interface())
		}
	}
	return res
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
