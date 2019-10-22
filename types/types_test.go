package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/xitongsys/parquet-go/parquet"
)

func TestStrToParquetType(t *testing.T) {
	testData := []struct {
		StrData string
		GoData  interface{}
		PT      *parquet.Type
		CT      *parquet.ConvertedType
		Length  int
		Scale   int
	}{
		{"false", bool(false), parquet.TypePtr(parquet.Type_BOOLEAN), nil, 0, 0},
		{"1", int32(1), parquet.TypePtr(parquet.Type_INT32), nil, 0, 0},
		{"0", int64(0), parquet.TypePtr(parquet.Type_INT64), nil, 0, 0},
		{"12345", StrIntToBinary("12345", "LittleEndian", 12, true), parquet.TypePtr(parquet.Type_INT96), nil, 0, 0},
		{"0.1", float32(0.1), parquet.TypePtr(parquet.Type_FLOAT), nil, 0, 0},
		{"0.1", float64(0.1), parquet.TypePtr(parquet.Type_DOUBLE), nil, 0, 0},
		{"abc bcd", string("abc bcd"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, 0, 0},
		{"abc bcd", string("abc bcd"), parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, 0, 0},

		{"abc bcd", string("abc bcd"), parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8), 0, 0},
		{"1", int32(1), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8), 0, 0},
		{"1", int32(1), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16), 0, 0},
		{"1", int32(1), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32), 0, 0},
		{"1", int64(1), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64), 0, 0},
		{"1", uint32(1), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8), 0, 0},
		{"1", uint32(1), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16), 0, 0},
		{"1", uint32(1), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32), 0, 0},
		{"1", uint64(1), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64), 0, 0},
		{"1", int32(1), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DATE), 0, 0},
		{"1", int32(1), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS), 0, 0},
		{"1", int64(1), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS), 0, 0},
		{"1", int64(1), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS), 0, 0},
		{"1", int64(1), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS), 0, 0},
		{"123456789", StrIntToBinary("123456789", "LittleEndian", 12, false), parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL), 0, 0},
		{"123.45", int32(12345), parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 12, 2},
		{"123.45", int64(12345), parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 12, 2},
		{"123.45", StrIntToBinary("12345", "BigEndian", 12, true), parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 12, 2},
		{"373.1145", StrIntToBinary("373114500000000000000", "BigEndian", 16, true), parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 16, 18},
		{"123.45", StrIntToBinary("12345", "BigEndian", 0, true), parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 12, 2},
		{"373.1145", StrIntToBinary("373114500000000000000", "BigEndian", 0, true), parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 16, 18},
	}

	for _, data := range testData {
		res := fmt.Sprintf("%v", StrToParquetType(data.StrData, data.PT, data.CT, data.Length, data.Scale))
		expect := fmt.Sprintf("%v", data.GoData)
		if res != expect {
			t.Errorf("StrToParquetType err %v-%v, expect %s, got %s", data.PT, data.CT, expect, res)
		}
	}
}

func TestTypeNameToParquetType(t *testing.T) {
	testData := []struct {
		name     string
		baseName string
		PT       *parquet.Type
		CT       *parquet.ConvertedType
	}{
		{"BOOLEAN", "", parquet.TypePtr(parquet.Type_BOOLEAN), nil},
		{"INT32", "", parquet.TypePtr(parquet.Type_INT32), nil},
		{"INT64", "", parquet.TypePtr(parquet.Type_INT64), nil},
		{"INT96", "", parquet.TypePtr(parquet.Type_INT96), nil},
		{"FLOAT", "", parquet.TypePtr(parquet.Type_FLOAT), nil},
		{"DOUBLE", "", parquet.TypePtr(parquet.Type_DOUBLE), nil},
		{"BYTE_ARRAY", "", parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil},
		{"FIXED_LEN_BYTE_ARRAY", "", parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil},

		{"UTF8", "", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)},
		{"INT_8", "", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8)},
		{"INT_16", "", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16)},
		{"INT_32", "", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)},
		{"INT_64", "", parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)},
		{"UINT_8", "", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8)},
		{"UINT_16", "", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16)},
		{"UINT_32", "", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32)},
		{"UINT_64", "", parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64)},
		{"DATE", "", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DATE)},
		{"TIME_MILLIS", "", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS)},
		{"TIME_MICROS", "", parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS)},
		{"TIMESTAMP_MICROS", "", parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS)},
		{"TIMESTAMP_MILLIS", "", parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS)},
		{"INTERVAL", "", parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL)},
		{"DECIMAL", "INT32", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)},
		{"DECIMAL", "INT64", parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)},
		{"DECIMAL", "FIXED_LEN_BYTE_ARRAY", parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)},
		{"DECIMAL", "BYTE_ARRAY", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)},
	}

	for _, data := range testData {
		pT, cT := TypeNameToParquetType(data.name, data.baseName)
		if *pT != *data.PT || (cT != nil && *cT != *data.CT) {
			t.Errorf("TypeNameToParquetType err, expect %v-%v, get %v-%v", *pT, *cT, *data.PT, *data.CT)
		}
	}
}

func TestStrIntToBinary(t *testing.T) {
	cases := []struct {
		num    int32
		nums   string
		order  string
		length int
		signed bool
	}{
		{0, "0", "LittleEndian", 4, true},
		{10, "10", "LittleEndian", 4, true},
		{-10, "-10", "LittleEndian", 4, true},
		{-111, "-111", "LittleEndian", 4, true},
		{2147483647, "2147483647", "LittleEndian", 0, true},
		{-2147483648, "-2147483648", "LittleEndian", 0, true},
		{-2147483648, "2147483648", "LittleEndian", 0, false},

		{0, "0", "BigEndian", 4, true},
		{10, "10", "BigEndian", 4, true},
		{-10, "-10", "BigEndian", 4, true},
		{-111, "-111", "BigEndian", 4, true},
		{2147483647, "2147483647", "BigEndian", 0, true},
		{-2147483648, "-2147483648", "BigEndian", 0, true},
		{-2147483648, "2147483648", "BigEndian", 0, false},
	}

	for _, c := range cases {
		buf := new(bytes.Buffer)
		if c.order == "LittleEndian" {
			binary.Write(buf, binary.LittleEndian, c.num)
		} else {
			binary.Write(buf, binary.BigEndian, c.num)
		}
		expect := string(buf.Bytes())

		res := StrIntToBinary(c.nums, c.order, c.length, c.signed)

		if res != expect {
			t.Errorf("StrIntToBinary error %b, expect %b, get %b", c.num, []byte(expect), []byte(res))
		}
	}
}
