package Common

/*
import (
	"fmt"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"reflect"
	"testing"
)

func TestHeadToUpper(t *testing.T) {
	testData := []struct {
		Str      string
		Expected string
	}{
		{"", ""},
		{"hello", "Hello"},
		{"HeHH", "HeHH"},
		{"a", "A"},
	}

	for _, data := range testData {
		res := HeadToUpper(data.Str)
		if res != data.Expected {
			t.Errorf("HeadToUpper err, expect %v, get %v", data.Expected, res)
		}
	}
}

func TestBitNum(t *testing.T) {
	testData := []struct {
		Num      uint64
		Expected uint64
	}{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 2},
		{8, 4},
	}

	for _, data := range testData {
		res := BitNum(data.Num)
		if res != data.Expected {
			t.Errorf("BitNum err, expect %v, get %v", data.Expected, res)
		}
	}
}

func TestCmp(t *testing.T) {
	testData := []struct {
		Num1, Num2 interface{}
		Expected   int
	}{
		{nil, nil, 0},
		{1, nil, 1},
		{nil, 0, -1},
		{BOOLEAN(true), BOOLEAN(false), 1},
		{BOOLEAN(false), BOOLEAN(false), 0},
		{BOOLEAN(true), BOOLEAN(true), 0},
		{BOOLEAN(false), BOOLEAN(true), -1},
		{INT32(2), INT32(1), 1},
		{INT32(2), INT32(3), -1},
		{INT32(2), INT32(2), 0},
		{INT64(2), INT64(1), 1},
		{INT64(2), INT64(3), -1},
		{INT64(2), INT64(2), 0},
		{INT96([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), INT96([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), 0},
		{INT96([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF}), INT96([]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0}), -1},
		{INT96([]byte{0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF}), INT96([]byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF}), 1},
		{FLOAT(-0.1), FLOAT(-0.1), 0},
		{FLOAT(-0.1), FLOAT(-0.0), -1},
		{FLOAT(-0.0), FLOAT(-0.1), 1},
		{DOUBLE(-0.1), DOUBLE(-0.1), 0},
		{DOUBLE(-0.1), DOUBLE(-0.0), -1},
		{DOUBLE(-0.0), DOUBLE(-0.1), 1},
		{BYTE_ARRAY("hello"), BYTE_ARRAY("hello"), 0},
		{BYTE_ARRAY("hello"), BYTE_ARRAY("hell"), 1},
		{BYTE_ARRAY(""), BYTE_ARRAY("hello"), -1},
		{FIXED_LEN_BYTE_ARRAY("hello"), FIXED_LEN_BYTE_ARRAY("hello"), 0},
		{FIXED_LEN_BYTE_ARRAY("hello"), FIXED_LEN_BYTE_ARRAY("hella"), 1},
		{FIXED_LEN_BYTE_ARRAY("hella"), FIXED_LEN_BYTE_ARRAY("hello"), -1},
		{UTF8("hello"), UTF8("hello"), 0},
		{UTF8("hello"), UTF8("hell"), 1},
		{UTF8(""), UTF8("hello"), -1},
		{INT_8(2), INT_8(1), 1},
		{INT_8(2), INT_8(3), -1},
		{INT_8(2), INT_8(2), 0},
		{INT_16(2), INT_16(1), 1},
		{INT_16(2), INT_16(3), -1},
		{INT_16(2), INT_16(2), 0},
		{INT_32(2), INT_32(1), 1},
		{INT_32(2), INT_32(3), -1},
		{INT_32(2), INT_32(2), 0},
		{INT_64(2), INT_64(1), 1},
		{INT_64(2), INT_64(3), -1},
		{INT_64(2), INT_64(2), 0},
		{UINT_8(2), UINT_8(1), 1},
		{UINT_8(2), UINT_8(3), -1},
		{UINT_8(2), UINT_8(2), 0},
		{UINT_16(2), UINT_16(1), 1},
		{UINT_16(2), UINT_16(3), -1},
		{UINT_16(2), UINT_16(2), 0},
		{UINT_32(2), UINT_32(1), 1},
		{UINT_32(2), UINT_32(3), -1},
		{UINT_32(2), UINT_32(2), 0},
		{UINT_64(2), UINT_64(1), 1},
		{UINT_64(2), UINT_64(3), -1},
		{UINT_64(2), UINT_64(2), 0},
		{DATE(2), DATE(1), 1},
		{DATE(2), DATE(3), -1},
		{DATE(2), DATE(2), 0},
		{TIME_MILLIS(2), TIME_MILLIS(1), 1},
		{TIME_MILLIS(2), TIME_MILLIS(3), -1},
		{TIME_MILLIS(2), TIME_MILLIS(2), 0},
		{TIME_MICROS(2), TIME_MICROS(1), 1},
		{TIME_MICROS(2), TIME_MICROS(3), -1},
		{TIME_MICROS(2), TIME_MICROS(2), 0},
		{TIMESTAMP_MILLIS(2), TIMESTAMP_MILLIS(1), 1},
		{TIMESTAMP_MILLIS(2), TIMESTAMP_MILLIS(3), -1},
		{TIMESTAMP_MILLIS(2), TIMESTAMP_MILLIS(2), 0},
		{TIMESTAMP_MICROS(2), TIMESTAMP_MICROS(1), 1},
		{TIMESTAMP_MICROS(2), TIMESTAMP_MICROS(3), -1},
		{TIMESTAMP_MICROS(2), TIMESTAMP_MICROS(2), 0},
		{INTERVAL([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), INTERVAL([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), 0},
		{INTERVAL([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF}), INTERVAL([]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0}), 1},
		{INTERVAL([]byte{0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF}), INTERVAL([]byte{0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF}), -1},
		{DECIMAL([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), DECIMAL([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), 0},
		{DECIMAL([]byte{0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), DECIMAL([]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0}), -1},
		{DECIMAL([]byte{0xFF, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), DECIMAL([]byte{0xFF, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), 1},
	}

	for _, data := range testData {
		res := Cmp(data.Num1, data.Num2)
		if res != data.Expected {
			t.Errorf("Cmp %v err, expect %v, get %v", reflect.TypeOf(data.Num1).Name(), data.Expected, res)
		}
	}
}

func TestMax(t *testing.T) {
	testData := []struct {
		Num1, Num2 interface{}
		Expected   interface{}
	}{
		{nil, 1, 1},
		{nil, nil, nil},
		{1, nil, 1},
		{1, 2, 2},
	}
	for _, data := range testData {
		res := Max(data.Num1, data.Num2)
		if res != data.Expected {
			t.Errorf("Max err, expect %v, get %v", data.Expected, res)
		}
	}
}

func TestMin(t *testing.T) {
	testData := []struct {
		Num1, Num2 interface{}
		Expected   interface{}
	}{
		{nil, 1, 1},
		{nil, nil, nil},
		{1, nil, 1},
		{1, 2, 1},
	}
	for _, data := range testData {
		res := Min(data.Num1, data.Num2)
		if res != data.Expected {
			t.Errorf("Min err, expect %v, get %v", data.Expected, res)
		}
	}
}

func TestSizeOf(t *testing.T) {
	testData := []struct {
		Value    reflect.Value
		Expected int64
	}{
		{reflect.ValueOf(BOOLEAN(true)), 1},
		{reflect.ValueOf(INT32(1)), 4},
		{reflect.ValueOf(INT64(1)), 8},
		{reflect.ValueOf(INT96("")), 12},
		{reflect.ValueOf(FLOAT(0.1)), 4},
		{reflect.ValueOf(DOUBLE(0.1)), 8},
		{reflect.ValueOf(BYTE_ARRAY("hello")), 5},
		{reflect.ValueOf(FIXED_LEN_BYTE_ARRAY("hello")), 5},
		{reflect.ValueOf(UTF8("hello")), 5},
		{reflect.ValueOf(INT_8(1)), 4},
		{reflect.ValueOf(INT_16(1)), 4},
		{reflect.ValueOf(INT_32(1)), 4},
		{reflect.ValueOf(INT_64(1)), 8},
		{reflect.ValueOf(UINT_8(1)), 4},
		{reflect.ValueOf(UINT_16(1)), 4},
		{reflect.ValueOf(UINT_32(1)), 4},
		{reflect.ValueOf(UINT_64(1)), 8},
		{reflect.ValueOf(DATE(1)), 4},
		{reflect.ValueOf(TIME_MILLIS(1)), 4},
		{reflect.ValueOf(TIME_MICROS(1)), 8},
		{reflect.ValueOf(TIMESTAMP_MILLIS(1)), 8},
		{reflect.ValueOf(TIMESTAMP_MICROS(1)), 8},
		{reflect.ValueOf(INTERVAL("")), 12},
		{reflect.ValueOf(DECIMAL("0123")), 4},
		{reflect.ValueOf(new(DECIMAL)), 0},
		{reflect.ValueOf((*DECIMAL)(nil)), 0},
		{reflect.ValueOf([]INT32{1, 2, 3}), 12},
		{reflect.ValueOf(map[UTF8]INT32{
			UTF8("1"):   1,
			UTF8("11"):  11,
			UTF8("111"): 111,
		}), 18},
		{reflect.ValueOf(struct {
			A INT32
			B INT64
			C []UTF8
			D map[UTF8]INT96
		}{
			1, 2, []UTF8{"hello", "world", "", "good"},
			map[UTF8]INT96{
				UTF8("hello"): INT96("012345678901"),
				UTF8("world"): INT96("012345678901"),
			},
		}), 60},
	}

	for _, data := range testData {
		res := SizeOf(data.Value)
		if res != data.Expected {
			t.Errorf("SizeOf err, expect %v, get %v", data.Expected, res)
		}
	}
}

func TestPathToStr(t *testing.T) {
	testData := []struct {
		Path     []string
		Expected string
	}{
		{[]string{"a", "b", "c"}, "a.b.c"},
		{[]string{"a", "", "c"}, "a..c"},
	}

	for _, data := range testData {
		res := PathToStr(data.Path)
		if res != data.Expected {
			t.Errorf("PathToStr err, expect %v, get %v", data.Expected, res)
		}
	}
}

func TestStrToPath(t *testing.T) {
	testData := []struct {
		Str      string
		Expected []string
	}{
		{"a.b.c", []string{"a", "b", "c"}},
		{"a..c", []string{"a", "", "c"}},
	}

	for _, data := range testData {
		res := StrToPath(data.Str)
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.Expected) {
			t.Errorf("PathToStr err, expect %v, get %v", data.Expected, res)
		}
	}
}

*/
