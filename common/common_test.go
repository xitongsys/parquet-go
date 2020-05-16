package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	"github.com/xitongsys/parquet-go/parquet"
	. "github.com/xitongsys/parquet-go/types"
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
		str    string
		numa   interface{}
		numb   interface{}
		PT     *parquet.Type
		CT     *parquet.ConvertedType
		expect bool
	}{
		{"bool 1", bool(false), bool(true), parquet.TypePtr(parquet.Type_BOOLEAN), nil, true},
		{"bool 2", bool(true), bool(false), parquet.TypePtr(parquet.Type_BOOLEAN), nil, false},
		{"bool 3", bool(true), bool(true), parquet.TypePtr(parquet.Type_BOOLEAN), nil, false},

		{"int32 1", int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), nil, true},
		{"int32 2", int32(-1), int32(2), parquet.TypePtr(parquet.Type_INT32), nil, true},

		{"int64 1", int64(-1), int64(-1), parquet.TypePtr(parquet.Type_INT64), nil, false},
		{"int64 2", int64(-1), int64(1), parquet.TypePtr(parquet.Type_INT64), nil, true},

		{"int96 1", string(StrIntToBinary("2147483648", "LittleEndian", 12, true)),
			string(StrIntToBinary("2147483647", "LittleEndian", 12, true)), parquet.TypePtr(parquet.Type_INT96), nil, false},
		{"int96 2", string(StrIntToBinary("-2147483648", "LittleEndian", 12, true)),
			string(StrIntToBinary("-2147483647", "LittleEndian", 12, true)), parquet.TypePtr(parquet.Type_INT96), nil, true},

		{"float 1", float32(0.1), float32(0.2), parquet.TypePtr(parquet.Type_FLOAT), nil, true},
		{"float 1", float32(0.1), float32(0.1), parquet.TypePtr(parquet.Type_FLOAT), nil, false},

		{"double 1", float64(0.1), float64(0.2), parquet.TypePtr(parquet.Type_DOUBLE), nil, true},
		{"double 2", float64(0.1), float64(0.1), parquet.TypePtr(parquet.Type_DOUBLE), nil, false},

		{"byte_array 1", string("abc bcd"), string("abc"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, false},
		{"byte_array 2", string("abc"), string("abc bcd"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, true},
		{"byte_array 3", string("abc bcd"), string("abc bcd"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, false},

		{"fixed 1", string("abc bcd"), string("abc aaa"), parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, false},
		{"fixed 2", string("abc"), string("bcd"), parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, true},
		{"fixed 3", string("abc bcd"), string("aac bcd"), parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, false},

		{"utf8 1", string("abc bcd"), string("abc"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8), false},
		{"utf8 2", string("abc"), string("abc"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8), false},
		{"utf8 3", string("abc"), string("abc def"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8), true},

		{"int_8 1", int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8), true},
		{"int_8 2", int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16), true},
		{"int_8 3", int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32), true},
		{"int_8 4", int64(1), int64(2), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64), true},

		{"uint_8 1", int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8), true},
		{"uint_8 2", int32(1), int32(-2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8), true},
		{"uint_8 3", int32(-1), int32(-2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8), false},
		{"uint_8 4", int32(-2), int32(-1), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8), true},
		{"uint_16 1", int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16), true},
		{"uint_16 2", int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32), true},
		{"uint_16 3", int64(1), int64(2), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64), true},

		{"date 1", int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DATE), true},
		{"time_millis 1", int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS), true},
		{"time_micros 1", int64(1), int64(2), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS), true},
		{"timestamp_micros 1", int64(1), int64(2), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS), true},
		{"timestamp_millis 1", int64(1), int64(2), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS), true},

		{"interval 1", string(StrIntToBinary("12345", "LittleEndian", 12, false)),
			string(StrIntToBinary("123456", "LittleEndian", 12, false)),
			parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL), true},
		{"interval 2", string(StrIntToBinary("123457", "LittleEndian", 12, false)),
			string(StrIntToBinary("123456", "LittleEndian", 12, false)),
			parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL), false},

		{"decimal 1", int32(12345), int32(123), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), false},
		{"decimal 2", int64(12345), int64(12346), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), true},

		{"decimal 3", string(StrIntToBinary("12345", "BigEndian", 0, true)),
			string(StrIntToBinary("12346", "BigEndian", 0, true)),
			parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), true},
		{"decimal 4", string(StrIntToBinary("-12345", "BigEndian", 0, true)),
			string(StrIntToBinary("-12346", "BigEndian", 0, true)),
			parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), false},

		{"decimal 5", string(StrIntToBinary("12345", "BigEndian", 0, true)),
			string(StrIntToBinary("12346", "BigEndian", 0, true)),
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), true},
		{"decimal 6", string(StrIntToBinary("-12345", "BigEndian", 0, true)),
			string(StrIntToBinary("-12346", "BigEndian", 0, true)),
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), false},
	}

	for _, c := range cases {
		funcTable := FindFuncTable(c.PT, c.CT)
		res := funcTable.LessThan(c.numa, c.numb)
		if res != c.expect {
			t.Errorf("Cmp error %v-%v, %v", c.numa, c.numa, c.str)
		}
	}
}

func TestMax(t *testing.T) {
	testData := []struct {
		Num1, Num2 interface{}
		PT         *parquet.Type
		CT         *parquet.ConvertedType
		Expected   interface{}
	}{
		{nil, int32(1), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		{nil, nil, parquet.TypePtr(parquet.Type_INT32), nil, nil},
		{int32(1), nil, parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		{int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), nil, int32(2)},
	}
	for _, data := range testData {
		funcTable := FindFuncTable(data.PT, data.CT)
		res := Max(funcTable, data.Num1, data.Num2)
		if res != data.Expected {
			t.Errorf("Max err, expect %v, get %v", data.Expected, res)
		}
	}
}

func TestMin(t *testing.T) {
	testData := []struct {
		Num1, Num2 interface{}
		PT         *parquet.Type
		CT         *parquet.ConvertedType
		Expected   interface{}
	}{
		{nil, int32(1), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		{nil, nil, parquet.TypePtr(parquet.Type_INT32), nil, nil},
		{int32(1), nil, parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		{int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
	}
	for _, data := range testData {
		funcTable := FindFuncTable(data.PT, data.CT)
		res := Min(funcTable, data.Num1, data.Num2)
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
		{reflect.ValueOf(bool(true)), 1},
		{reflect.ValueOf(int32(1)), 4},
		{reflect.ValueOf(int64(1)), 8},
		{reflect.ValueOf(string("012345678901")), 12},
		{reflect.ValueOf(float32(0.1)), 4},
		{reflect.ValueOf(float64(0.1)), 8},
		{reflect.ValueOf(string("hello")), 5},
		{reflect.ValueOf(string("hello")), 5},
		{reflect.ValueOf(string("hello")), 5},
		{reflect.ValueOf(int32(1)), 4},
		{reflect.ValueOf(int32(1)), 4},
		{reflect.ValueOf(int32(1)), 4},
		{reflect.ValueOf(int64(1)), 8},
		{reflect.ValueOf(int32(1)), 4},
		{reflect.ValueOf(int32(1)), 4},
		{reflect.ValueOf(int32(1)), 4},
		{reflect.ValueOf(int64(1)), 8},
		{reflect.ValueOf(int64(1)), 8},
		{reflect.ValueOf(int32(1)), 4},
		{reflect.ValueOf(int64(1)), 8},
		{reflect.ValueOf(int64(1)), 8},
		{reflect.ValueOf(int64(1)), 8},
		{reflect.ValueOf(string("012345678901")), 12},
		{reflect.ValueOf(string("0123")), 4},
		{reflect.ValueOf(new(string)), 0},
		{reflect.ValueOf((*string)(nil)), 0},
		{reflect.ValueOf([]int32{1, 2, 3}), 12},
		{reflect.ValueOf(map[string]int32{
			string("1"):   1,
			string("11"):  11,
			string("111"): 111,
		}), 18},
		{reflect.ValueOf(struct {
			A int32
			B int64
			C []string
			D map[string]string
		}{
			1, 2, []string{"hello", "world", "", "good"},
			map[string]string{
				string("hello"): string("012345678901"),
				string("world"): string("012345678901"),
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
