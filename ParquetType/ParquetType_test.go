package ParquetType

import (
	"bytes"
	"encoding/binary"
	"testing"
)

/*
func TestStrToParquetType(t *testing.T) {
	testData := []struct {
		StrData     string
		PT          *parquet.Type
		CT          *parquet.ConvertedType
		ParquetData interface{}
	}{
		{"false", parquet.TypePtr(parquet.Type_BOOLEAN), nil, BOOLEAN(false)},
		{"1", "INT32", INT32(1)},
		{"0", "INT64", INT64(0)},
		{"012345678901", "INT96", INT96("012345678901")},
		{"0.1", "FLOAT", FLOAT(0.1)},
		{"0.1", "DOUBLE", DOUBLE(0.1)},
		{"abc bcd", "BYTE_ARRAY", BYTE_ARRAY("abc bcd")},
		{"abc bcd", "FIXED_LEN_BYTE_ARRAY", FIXED_LEN_BYTE_ARRAY("abc bcd")},
		{"abc bcd", "UTF8", UTF8("abc bcd")},
		{"1", "INT_8", INT_8(1)},
		{"1", "INT_16", INT_16(1)},
		{"1", "INT_32", INT_32(1)},
		{"1", "INT_64", INT_64(1)},
		{"1", "UINT_8", UINT_8(1)},
		{"1", "UINT_16", UINT_16(1)},
		{"1", "UINT_32", UINT_32(1)},
		{"1", "UINT_64", UINT_64(1)},
		{"1", "DATE", DATE(1)},
		{"1", "TIME_MILLIS", TIME_MILLIS(1)},
		{"1", "TIME_MICROS", TIME_MICROS(1)},
		{"1", "TIMESTAMP_MICROS", TIMESTAMP_MICROS(1)},
		{"1", "TIMESTAMP_MILLIS", TIMESTAMP_MILLIS(1)},
		{"012345678901", "INTERVAL", INTERVAL("012345678901")},
		{"012345678901", "DECIMAL", DECIMAL("012345678901")},
		{"", "hehe", nil},
	}

	for _, data := range testData {
		res := StrToParquetType(data.StrData, data.Type)
		if res != data.ParquetData {
			t.Errorf("StrToParquetType err, expect %v, get %v", data.ParquetData, res)
		}
	}
}

func TestParquetTypeToGoType(t *testing.T) {
	testData := []struct {
		StrData     string
		Type        string
		ParquetData interface{}
	}{
		{"false", "BOOLEAN", BOOLEAN(false)},
		{"1", "INT32", INT32(1)},
		{"0", "INT64", INT64(0)},
		{"012345678901", "INT96", INT96("012345678901")},
		{"0.1", "FLOAT", FLOAT(0.1)},
		{"0.1", "DOUBLE", DOUBLE(0.1)},
		{"abc bcd", "BYTE_ARRAY", BYTE_ARRAY("abc bcd")},
		{"abc bcd", "FIXED_LEN_BYTE_ARRAY", FIXED_LEN_BYTE_ARRAY("abc bcd")},
		{"abc bcd", "UTF8", UTF8("abc bcd")},
		{"1", "INT_8", INT_8(1)},
		{"1", "INT_16", INT_16(1)},
		{"1", "INT_32", INT_32(1)},
		{"1", "INT_64", INT_64(1)},
		{"1", "UINT_8", UINT_8(1)},
		{"1", "UINT_16", UINT_16(1)},
		{"1", "UINT_32", UINT_32(1)},
		{"1", "UINT_64", UINT_64(1)},
		{"1", "DATE", DATE(1)},
		{"1", "TIME_MILLIS", TIME_MILLIS(1)},
		{"1", "TIME_MICROS", TIME_MICROS(1)},
		{"1", "TIMESTAMP_MICROS", TIMESTAMP_MICROS(1)},
		{"1", "TIMESTAMP_MILLIS", TIMESTAMP_MILLIS(1)},
		{"012345678901", "INTERVAL", INTERVAL("012345678901")},
		{"012345678901", "DECIMAL", DECIMAL("012345678901")},
	}

	for _, data := range testData {
		res := fmt.Sprintf("%v", ParquetTypeToGoType(data.ParquetData))
		if res != data.StrData {
			t.Errorf("ParquetTypeToGoType err, expect %v, get %v", data.StrData, res)
		}
	}
}
*/

func TestStrIntToBinary(t *testing.T) {
	cases := []struct {
		num    int32
		nums   string
		order  string
		length int32
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
			t.Errorf("StrIntToBinary error %b, expect %v, get %v", c.num, len(expect), len(res))
		}
	}
}
