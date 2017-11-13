package ParquetType

import (
	"testing"
)

/*
func TestNameToBaseType(t *testing.T) {
	testData := []struct {
		Name string
		Type parquet.Type
	}{
		{"BOOLEAN", parquet.Type_BOOLEAN},
		{"INT32", parquet.Type_INT32},
		{"INT64", parquet.Type_INT64},
		{"INT96", parquet.Type_INT96},
		{"FLOAT", parquet.Type_FLOAT},
		{"DOUBLE", parquet.Type_DOUBLE},
		{"BYTE_ARRAY", parquet.Type_BYTE_ARRAY},
		{"FIXED_LEN_BYTE_ARRAY", parquet.Type_FIXED_LEN_BYTE_ARRAY},
	}

	for _, data := range testData {
		res := NameToBaseType(data.Name)
		if res != data.Type {
			t.Errorf("NameToBaseType err, expect %v, get %v", data.Type, res)
		}
	}
}

func TestNameToConvertedType(t *testing.T) {
	testData := []struct {
		Name string
		Type parquet.ConvertedType
	}{
		{"UTF8", parquet.ConvertedType_UTF8},
		{"INT_8", parquet.ConvertedType_INT_8},
		{"INT_16", parquet.ConvertedType_INT_16},
		{"INT_32", parquet.ConvertedType_INT_32},
		{"INT_64", parquet.ConvertedType_INT_64},
		{"UINT_8", parquet.ConvertedType_UINT_8},
		{"UINT_16", parquet.ConvertedType_UINT_16},
		{"UINT_32", parquet.ConvertedType_UINT_32},
		{"UINT_64", parquet.ConvertedType_UINT_64},
		{"DATE", parquet.ConvertedType_DATE},
		{"TIME_MILLIS", parquet.ConvertedType_TIME_MILLIS},
		{"TIME_MICROS", parquet.ConvertedType_TIME_MICROS},
		{"TIMESTAMP_MILLIS", parquet.ConvertedType_TIMESTAMP_MILLIS},
		{"TIMESTAMP_MICROS", parquet.ConvertedType_TIMESTAMP_MICROS},
		{"INTERVAL", parquet.ConvertedType_INTERVAL},
		{"DECIMAL", parquet.ConvertedType_DECIMAL},
		{"hehe", parquet.ConvertedType_UTF8},
	}

	for _, data := range testData {
		res := NameToConvertedType(data.Name)
		if res != data.Type {
			t.Errorf("NameToBaseType err, expect %v, get %v", data.Type, res)
		}
	}
}

func TestIsBaseType(t *testing.T) {
	testData := []struct {
		Name string
		Flag bool
	}{
		{"BOOLEAN", true},
		{"INT32", true},
		{"INT64", true},
		{"INT96", true},
		{"FLOAT", true},
		{"DOUBLE", true},
		{"BYTE_ARRAY", true},
		{"FIXED_LEN_BYTE_ARRAY", true},
		{"UTF8", false},
		{"INT_8", false},
		{"INT_16", false},
		{"INT_32", false},
		{"INT_64", false},
		{"UINT_8", false},
		{"UINT_16", false},
		{"UINT_32", false},
		{"UINT_64", false},
		{"DATE", false},
		{"TIME_MILLIS", false},
		{"TIME_MICROS", false},
		{"TIMESTAMP_MILLIS", false},
		{"TIMESTAMP_MICROS", false},
		{"INTERAL", false},
		{"DECIMAL", false},
	}

	for _, data := range testData {
		res := IsBaseType(data.Name)
		if res != data.Flag {
			t.Errorf("IsBaseType err")
		}
	}
}
*/

func TestStrToParquetType(t *testing.T) {
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
		{"", "hehe", nil},
	}

	for _, data := range testData {
		res := StrToParquetType(data.StrData, data.Type)
		if res != data.ParquetData {
			t.Errorf("StrToParquetType err, expect %v, get %v", data.ParquetData, res)
		}
	}
}
