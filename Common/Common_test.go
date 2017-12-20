package Common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/parquet"
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

func TestCmpIntBinary(t *testing.T) {
	cases := []struct {
		numa int32
		numb int32
	}{
		{-1, 0},
		{1, 2},
		{1, 1},
		{1, 0},
		{0, 0},
		{-1, -2},
		{-2, -1},
		{-1, 1},
		{2147483647, 2147483647},
		{-2147483648, -2147483647},
		{-2147483648, 2147483647},
	}

	for _, c := range cases {
		abuf, bbuf := new(bytes.Buffer), new(bytes.Buffer)
		binary.Write(abuf, binary.LittleEndian, c.numa)
		binary.Write(bbuf, binary.LittleEndian, c.numb)
		as, bs := string(abuf.Bytes()), string(bbuf.Bytes())
		if (c.numa < c.numb) != (CmpIntBinary(as, bs, "LittleEndian", true)) {
			t.Errorf("CmpIntBinary error, %v-%v", c.numa, c.numb)
		}
	}

	cases2 := []struct {
		numa string
		numb string
	}{
		{"-1", "0"},
		{"1", "2"},
		{"1", "1"},
		{"1", "0"},
		{"0", "0"},
		{"-123", "-2"},
		{"-2", "-1"},
		{"-1344", "123"},
		{"2147483647", "2147483647"},
		{"-2147483648", "-2147483647"},
		{"-2147483648", "2147483647"},
	}

	for _, c := range cases2 {
		as := StrIntToBinary(c.numa, "LittleEndian", 0, true)
		bs := StrIntToBinary(c.numb, "LittleEndian", 0, true)
		an, bn := 0, 0
		fmt.Sscanf(c.numa, "%d", &an)
		fmt.Sscanf(c.numb, "%d", &bn)
		if (an < bn) != (CmpIntBinary(as, bs, "LittleEndian", true)) {
			t.Errorf("CmpIntBinary error, %v-%v", c.numa, c.numb)
		}
	}

	cases3 := []struct {
		numa string
		numb string
	}{
		{"1", "2"},
		{"1", "1"},
		{"1", "0"},
		{"0", "0"},
		{"123", "2"},
		{"1344", "123"},
		{"2147483647", "2147483647"},
		{"2147483648", "2147483647"},
	}

	for _, c := range cases3 {
		as := StrIntToBinary(c.numa, "LittleEndian", 0, false)
		bs := StrIntToBinary(c.numb, "LittleEndian", 0, false)
		an, bn := uint64(0), uint64(0)
		fmt.Sscanf(c.numa, "%d", &an)
		fmt.Sscanf(c.numb, "%d", &bn)
		if (an < bn) != (CmpIntBinary(as, bs, "LittleEndian", false)) {
			t.Errorf("CmpIntBinary error, %v-%v", c.numa, c.numb)
		}
	}
}

func TestCmp(t *testing.T) {
	cases := []struct {
		strdata string
		numa    interface{}
		numb    interface{}
		PT      *parquet.Type
		CT      *parquet.ConvertedType
		expect  bool
	}{
		{"false", BOOLEAN(false), BOOLEAN(true), parquet.TypePtr(parquet.Type_BOOLEAN), nil, true},
		{"false", BOOLEAN(true), BOOLEAN(false), parquet.TypePtr(parquet.Type_BOOLEAN), nil, false},
		{"false", BOOLEAN(true), BOOLEAN(true), parquet.TypePtr(parquet.Type_BOOLEAN), nil, false},

		{"1", INT32(1), INT32(2), parquet.TypePtr(parquet.Type_INT32), nil, true},
		{"1", INT32(-1), INT32(2), parquet.TypePtr(parquet.Type_INT32), nil, true},

		{"1", INT64(-1), INT64(-1), parquet.TypePtr(parquet.Type_INT64), nil, false},
		{"1", INT64(-1), INT64(1), parquet.TypePtr(parquet.Type_INT64), nil, true},

		{"012345678901", INT96(StrIntToBinary("2147483648", "LittleEndian", 12, true)),
			INT96(StrIntToBinary("2147483647", "LittleEndian", 12, true)), parquet.TypePtr(parquet.Type_INT96), nil, false},
		{"012345678901", INT96(StrIntToBinary("-2147483648", "LittleEndian", 12, true)),
			INT96(StrIntToBinary("-2147483647", "LittleEndian", 12, true)), parquet.TypePtr(parquet.Type_INT96), nil, true},

		{"0.1", FLOAT(0.1), FLOAT(0.2), parquet.TypePtr(parquet.Type_FLOAT), nil, true},
		{"0.1", FLOAT(0.1), FLOAT(0.1), parquet.TypePtr(parquet.Type_FLOAT), nil, false},

		{"0.1", DOUBLE(0.1), DOUBLE(0.2), parquet.TypePtr(parquet.Type_DOUBLE), nil, true},
		{"0.1", DOUBLE(0.1), DOUBLE(0.1), parquet.TypePtr(parquet.Type_DOUBLE), nil, false},

		{"abc bcd", BYTE_ARRAY("abc bcd"), BYTE_ARRAY("abc"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, false},
		{"abc bcd", BYTE_ARRAY("abc"), BYTE_ARRAY("abc bcd"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, true},
		{"abc bcd", BYTE_ARRAY("abc bcd"), BYTE_ARRAY("abc bcd"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, false},

		{"abc bcd", FIXED_LEN_BYTE_ARRAY("abc bcd"), FIXED_LEN_BYTE_ARRAY("abc aaa"), parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, false},
		{"abc bcd", FIXED_LEN_BYTE_ARRAY("abc"), FIXED_LEN_BYTE_ARRAY("bcd"), parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, true},
		{"abc bcd", FIXED_LEN_BYTE_ARRAY("abc bcd"), FIXED_LEN_BYTE_ARRAY("aac bcd"), parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, false},

		{"abc bcd", BYTE_ARRAY("abc bcd"), BYTE_ARRAY("abc"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8), false},
		{"abc bcd", BYTE_ARRAY("abc"), BYTE_ARRAY("abc"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8), false},
		{"abc bcd", BYTE_ARRAY("abc"), BYTE_ARRAY("abc def"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8), true},

		{"1", INT32(1), INT32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8), true},
		{"1", INT32(1), INT32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16), true},
		{"1", INT32(1), INT32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32), true},
		{"1", INT64(1), INT64(2), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64), true},

		{"1", INT32(1), INT32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8), true},
		{"1", INT32(1), INT32(-2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8), true},
		{"1", INT32(-1), INT32(-2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8), true},
		{"1", INT32(-2), INT32(-1), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8), false},
		{"1", INT32(1), INT32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16), true},
		{"1", INT32(1), INT32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32), true},
		{"1", INT64(1), INT64(2), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64), true},

		{"1", INT32(1), INT32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DATE), true},
		{"1", INT32(1), INT32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS), true},
		{"1", INT64(1), INT64(2), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS), true},
		{"1", INT64(1), INT64(2), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS), true},
		{"1", INT64(1), INT64(2), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS), true},

		{"012345678901", FIXED_LEN_BYTE_ARRAY(StrIntToBinary("12345", "LittleEndian", 12, false)),
			FIXED_LEN_BYTE_ARRAY(StrIntToBinary("123456", "LittleEndian", 12, false)),
			parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL), true},
		{"012345678901", FIXED_LEN_BYTE_ARRAY(StrIntToBinary("123457", "LittleEndian", 12, false)),
			FIXED_LEN_BYTE_ARRAY(StrIntToBinary("123456", "LittleEndian", 12, false)),
			parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL), false},

		{"12345", INT32(12345), INT32(123), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), false},
		{"12345", INT64(12345), INT64(12346), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), true},

		{"12345", FIXED_LEN_BYTE_ARRAY(StrIntToBinary("12345", "BigEndian", 0, true)),
			FIXED_LEN_BYTE_ARRAY(StrIntToBinary("12346", "BigEndian", 0, true)),
			parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), true},
		{"12345", FIXED_LEN_BYTE_ARRAY(StrIntToBinary("-12345", "BigEndian", 0, true)),
			FIXED_LEN_BYTE_ARRAY(StrIntToBinary("-12346", "BigEndian", 0, true)),
			parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), false},

		{"12345", BYTE_ARRAY(StrIntToBinary("12345", "BigEndian", 0, true)),
			BYTE_ARRAY(StrIntToBinary("12346", "BigEndian", 0, true)),
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), true},
		{"12345", BYTE_ARRAY(StrIntToBinary("-12345", "BigEndian", 0, true)),
			BYTE_ARRAY(StrIntToBinary("-12346", "BigEndian", 0, true)),
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), false},
	}

	for _, c := range cases {
		res := Cmp(c.numa, c.numb, c.PT, c.CT)
		fmt.Println("====", c.PT, c.CT)
		if res != c.expect {
			t.Errorf("Cmp error %v-%v", c.numa, c.numa)
		}
	}
}

/*
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
*/

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
