package types

import (
	"testing"
	"time"

	"github.com/hangxie/parquet-go/v2/parquet"
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

	a8, _ := StrToParquetType("-12.345", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 0, 3)
	sa8 := DECIMAL_INT_ToString(int64(a8.(int32)), 0, 3)
	if sa8 != "-12.345" {
		t.Error("DECIMAL_INT_ToString error: ", a8, sa8)
	}

	a9, _ := StrToParquetType("-0.001", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 0, 3)
	sa9 := DECIMAL_INT_ToString(int64(a9.(int32)), 0, 3)
	if sa9 != "-0.001" {
		t.Error("DECIMAL_INT_ToString error: ", a9, sa9)
	}

	a10, _ := StrToParquetType("0.0001", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 0, 4)
	sa10 := DECIMAL_INT_ToString(int64(a10.(int32)), 0, 4)
	if sa10 != "0.0001" {
		t.Error("DECIMAL_INT_ToString error: ", a10, sa10)
	}

	a11, _ := StrToParquetType("-100000", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa11 := DECIMAL_BYTE_ARRAY_ToString([]byte(a11.(string)), 8, 4)
	if sa11 != "-100000.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a11, sa11)
	}

	a12, _ := StrToParquetType("100000", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa12 := DECIMAL_BYTE_ARRAY_ToString([]byte(a12.(string)), 8, 4)
	if sa12 != "100000.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a12, sa12)
	}

	a13, _ := StrToParquetType("-100", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa13 := DECIMAL_BYTE_ARRAY_ToString([]byte(a13.(string)), 8, 4)
	if sa13 != "-100.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a13, sa13)
	}

	a14, _ := StrToParquetType("100", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa14 := DECIMAL_BYTE_ARRAY_ToString([]byte(a14.(string)), 8, 4)
	if sa14 != "100.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a14, sa14)
	}

	a15, _ := StrToParquetType("-431", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa15 := DECIMAL_BYTE_ARRAY_ToString([]byte(a15.(string)), 8, 4)
	if sa15 != "-431.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a15, sa15)
	}

	a16, _ := StrToParquetType("431", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa16 := DECIMAL_BYTE_ARRAY_ToString([]byte(a16.(string)), 8, 4)
	if sa16 != "431.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a16, sa16)
	}

	a17, _ := StrToParquetType("-432", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa17 := DECIMAL_BYTE_ARRAY_ToString([]byte(a17.(string)), 8, 4)
	if sa17 != "-432.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a17, sa17)
	}

	a18, _ := StrToParquetType("432", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa18 := DECIMAL_BYTE_ARRAY_ToString([]byte(a18.(string)), 8, 4)
	if sa18 != "432.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a18, sa18)
	}

	a19, _ := StrToParquetType("-433", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa19 := DECIMAL_BYTE_ARRAY_ToString([]byte(a19.(string)), 8, 4)
	if sa19 != "-433.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a19, sa19)
	}

	a20, _ := StrToParquetType("433", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa20 := DECIMAL_BYTE_ARRAY_ToString([]byte(a20.(string)), 8, 4)
	if sa20 != "433.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a20, sa20)
	}

	a21, _ := StrToParquetType("-65535", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa21 := DECIMAL_BYTE_ARRAY_ToString([]byte(a21.(string)), 8, 4)
	if sa21 != "-65535.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a21, sa21)
	}

	a22, _ := StrToParquetType("65535", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa22 := DECIMAL_BYTE_ARRAY_ToString([]byte(a22.(string)), 8, 4)
	if sa22 != "65535.0000" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a22, sa22)
	}
}
