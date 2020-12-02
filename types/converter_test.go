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
	a1 := StrToParquetType("1.23", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 2).(int32)
	sa1 := DECIMAL_INT_ToString(int64(a1), 9, 2)
	if sa1 != "1.23" {
		t.Error("DECIMAL_INT_ToString error: ", a1, sa1)
	}

	a2 := StrToParquetType("1.230", parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3).(int64)
	sa2 := DECIMAL_INT_ToString(int64(a2), 9, 3)
	if sa2 != "1.230" {
		t.Error("DECIMAL_INT_ToString error: ", a2, sa2)
	}

	a3 := StrToParquetType("11.230", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3).(string)
	sa3 := DECIMAL_BYTE_ARRAY_ToString([]byte(a3), 9, 3)
	if sa3 != "11.230" {
		t.Error("DECIMAL_BYTE_ARRAY_ToString error: ", a3, sa3)
	}

}