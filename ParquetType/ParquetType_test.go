package ParquetType

import (
	"github.com/xitongsys/parquet-go/parquet"
	"testing"
)

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
