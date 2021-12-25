package types

import (
	"testing"
	"time"

	"github.com/xitongsys/parquet-go/parquet"
)

func TestINT96(t *testing.T) {
	t1 := time.Now().Truncate(time.Microsecond).UTC()
	s := TimeToINT96(t1)
	t2 := INT96ToTime(s)

	if !t1.Equal(t2) {
		t.Error("INT96 error: ", t1, t2)
	}

}

func TestDECIMAL(t *testing.T) {
	a1, _ := StrToParquetType("1.23", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 2)
	sa1 := DECIMAL_INT_ToString(int64(a1.(int32)), 9, 2)
	if sa1 != "1.23" {
		t.Error("DECIMAL_INT_ToString error: ", a1, sa1)
	}

	a2, _ := StrToParquetType("1.230", parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa2 := DECIMAL_INT_ToString(int64(a2.(int64)), 9, 3)
	if sa2 != "1.230" {
		t.Error("DECIMAL_INT_ToString error: ", a2, sa2)
	}

	a3, _ := StrToParquetType("11.230", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa3 := DECIMAL_BYTE_ARRAY_ToString([]byte(a3.(string)), 9, 3)
	if sa3 != "11.230" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a3, sa3)
	}

	a4, _ := StrToParquetType("-123.456", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa4 := DECIMAL_BYTE_ARRAY_ToString([]byte(a4.(string)), 9, 3)
	if sa4 != "-123.456" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a4, sa4)
	}

	a5, _ := StrToParquetType("0.000", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa5 := DECIMAL_BYTE_ARRAY_ToString([]byte(a5.(string)), 9, 3)
	if sa5 != "0.000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a5, sa5)
	}

	a6, _ := StrToParquetType("-0.01", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 6, 2)
	sa6 := DECIMAL_BYTE_ARRAY_ToString([]byte(a6.(string)), 6, 2)
	if sa6 != "-0.01" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a6, sa6)
	}

	a7, _ := StrToParquetType("0.1234", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa7 := DECIMAL_BYTE_ARRAY_ToString([]byte(a7.(string)), 8, 4)
	if sa7 != "0.1234" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a7, sa7)
	}
}
