package ParquetType

/*
import (
	"fmt"
	"testing"
)


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
