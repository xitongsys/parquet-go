package Common

import (
	. "github.com/xitongsys/parquet-go/ParquetType"
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

func TestCmp(t *testing.T) {
	testData := []struct {
		Num1, Num2 interface{}
		Expected   int
	}{
		{nil, nil, 0},
		{1, nil, 1},
		{nil, 0, -1},
		{BOOLEAN(true), BOOLEAN(false), 1},
		{BOOLEAN(false), BOOLEAN(false), 0},
		{BOOLEAN(true), BOOLEAN(true), 0},
		{BOOLEAN(false), BOOLEAN(true), -1},
		{INT32(2), INT32(1), 1},
		{INT32(2), INT32(3), -1},
		{INT32(2), INT32(2), 0},
		{INT64(2), INT64(1), 1},
		{INT64(2), INT64(3), -1},
		{INT64(2), INT64(2), 0},
		{INT96([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), INT96([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), 0},
		{INT96([]byte{0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), INT96([]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0}), -1},
		{INT96([]byte{0xFF, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), INT96([]byte{0xFF, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), 1},
	}
	for _, data := range testData {
		res := Cmp(data.Num1, data.Num2)
		if res != data.Expected {
			t.Errorf("Cmp err, expect %v, get %v", data.Expected, res)
		}
	}
}

func TestSizeOf(t *testing.T) {

}

func TestTypeNumberField(t *testing.T) {

}

func TestGoTypeToParquetType(t *testing.T) {

}
