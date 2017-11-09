package PEncoding

import (
	"bytes"
	"fmt"
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"testing"
)

func TestReadPlainINT32(t *testing.T) {
	testData := []struct {
		expected   []interface{}
		byteReader *bytes.Reader
	}{
		{[]interface{}{}, bytes.NewReader([]byte{})},
		{[]interface{}{INT32(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{INT32(0), INT32(1), INT32(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0})},
	}

	for _, data := range testData {
		res := ReadPlainINT32(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainINT32 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainINT64(t *testing.T) {
	testData := []struct {
		expected   []interface{}
		byteReader *bytes.Reader
	}{
		{[]interface{}{}, bytes.NewReader([]byte{})},
		{[]interface{}{INT64(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{INT64(0), INT64(1), INT64(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0})},
	}

	for _, data := range testData {
		res := ReadPlainINT64(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainINT64 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainINT_8(t *testing.T) {
	testData := []struct {
		expected   []interface{}
		byteReader *bytes.Reader
	}{
		{[]interface{}{}, bytes.NewReader([]byte{})},
		{[]interface{}{INT_8(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{INT_8(0), INT_8(1), INT_8(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0})},
	}

	for _, data := range testData {
		res := ReadPlainINT_8(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainINT_8 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainUINT_8(t *testing.T) {
	testData := []struct {
		expected   []interface{}
		byteReader *bytes.Reader
	}{
		{[]interface{}{}, bytes.NewReader([]byte{})},
		{[]interface{}{UINT_8(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{UINT_8(0), UINT_8(1), UINT_8(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0})},
	}

	for _, data := range testData {
		res := ReadPlainUINT_8(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainUINT_8 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainINT_16(t *testing.T) {
	testData := []struct {
		expected   []interface{}
		byteReader *bytes.Reader
	}{
		{[]interface{}{}, bytes.NewReader([]byte{})},
		{[]interface{}{INT_16(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{INT_16(0), INT_16(1), INT_16(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0})},
	}

	for _, data := range testData {
		res := ReadPlainINT_16(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainINT_16 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainUINT_16(t *testing.T) {
	testData := []struct {
		expected   []interface{}
		byteReader *bytes.Reader
	}{
		{[]interface{}{}, bytes.NewReader([]byte{})},
		{[]interface{}{UINT_16(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{UINT_16(0), UINT_16(1), UINT_16(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0})},
	}

	for _, data := range testData {
		res := ReadPlainUINT_16(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainUINT_16 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainINT_32(t *testing.T) {
	testData := []struct {
		expected   []interface{}
		byteReader *bytes.Reader
	}{
		{[]interface{}{}, bytes.NewReader([]byte{})},
		{[]interface{}{INT_32(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{INT_32(0), INT_32(1), INT_32(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0})},
	}

	for _, data := range testData {
		res := ReadPlainINT_32(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainINT_32 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainUINT_32(t *testing.T) {
	testData := []struct {
		expected   []interface{}
		byteReader *bytes.Reader
	}{
		{[]interface{}{}, bytes.NewReader([]byte{})},
		{[]interface{}{UINT_32(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{UINT_32(0), UINT_32(1), UINT_32(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0})},
	}

	for _, data := range testData {
		res := ReadPlainUINT_32(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainUINT_32 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainINT_64(t *testing.T) {
	testData := []struct {
		expected   []interface{}
		byteReader *bytes.Reader
	}{
		{[]interface{}{}, bytes.NewReader([]byte{})},
		{[]interface{}{INT_64(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{INT_64(0), INT_64(1), INT_64(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0})},
	}

	for _, data := range testData {
		res := ReadPlainINT_64(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainINT_64 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainUINT_64(t *testing.T) {
	testData := []struct {
		expected   []interface{}
		byteReader *bytes.Reader
	}{
		{[]interface{}{}, bytes.NewReader([]byte{})},
		{[]interface{}{UINT_64(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{UINT_64(0), UINT_64(1), UINT_64(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0})},
	}

	for _, data := range testData {
		res := ReadPlainUINT_64(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainUINT_64 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainBYTE_ARRAY(t *testing.T) {
	testData := [][]interface{}{
		{BYTE_ARRAY("hello"), BYTE_ARRAY("world")},
		{BYTE_ARRAY("good"), BYTE_ARRAY(""), BYTE_ARRAY("a"), BYTE_ARRAY("b")},
	}

	for _, data := range testData {
		res := ReadPlainBYTE_ARRAY(bytes.NewReader(WritePlainBYTE_ARRAY(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainBYTE_ARRAY err, %v", data)
		}
	}
}

func TestReadPlainFIXED_LEN_BYTE_ARRAY(t *testing.T) {
	testData := [][]interface{}{
		{FIXED_LEN_BYTE_ARRAY("hello"), FIXED_LEN_BYTE_ARRAY("world")},
		{FIXED_LEN_BYTE_ARRAY("a"), FIXED_LEN_BYTE_ARRAY("b"), FIXED_LEN_BYTE_ARRAY("c"), FIXED_LEN_BYTE_ARRAY("d")},
	}

	for _, data := range testData {
		res := ReadPlainFIXED_LEN_BYTE_ARRAY(bytes.NewReader(WritePlainFIXED_LEN_BYTE_ARRAY(data)), uint64(len(data)), uint64(len(data[0].(FIXED_LEN_BYTE_ARRAY))))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainFIXED_LEN_BYTE_ARRAY err, %v", data)
		}
	}
}

func TestReadPlainUTF8(t *testing.T) {
	testData := [][]interface{}{
		{UTF8("hello"), UTF8("world")},
		{UTF8("a"), UTF8("b"), UTF8("c"), UTF8("d")},
	}
	for _, data := range testData {
		res := ReadPlainUTF8(bytes.NewReader(WritePlainUTF8(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainUTF8 err, %v", data)
		}
	}
}

func TestReadPlainDATE(t *testing.T) {
	testData := [][]interface{}{
		{DATE(0), DATE(1), DATE(2)},
		{DATE(0), DATE(0), DATE(0)},
	}

	for _, data := range testData {
		res := ReadPlainDATE(bytes.NewReader(WritePlainDATE(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainDATE err, %v", data)
		}
	}
}

func TestReadPlainTIME_MILLIS(t *testing.T) {
	testData := [][]interface{}{
		{TIME_MILLIS(0), TIME_MILLIS(1), TIME_MILLIS(2)},
		{TIME_MILLIS(0), TIME_MILLIS(0), TIME_MILLIS(0)},
	}

	for _, data := range testData {
		res := ReadPlainTIME_MILLIS(bytes.NewReader(WritePlainTIME_MILLIS(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainTIME_MILLIS err, %v", data)
		}
	}
}

func TestReadPlainTIME_MICROS(t *testing.T) {
	testData := [][]interface{}{
		{TIME_MICROS(0), TIME_MICROS(1), TIME_MICROS(2)},
		{TIME_MICROS(0), TIME_MICROS(0), TIME_MICROS(0)},
	}

	for _, data := range testData {
		res := ReadPlainTIME_MICROS(bytes.NewReader(WritePlainTIME_MICROS(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainTIME_MICROS err, %v", data)
		}
	}
}

func TestReadPlainTIMESTAMP_MILLIS(t *testing.T) {
	testData := [][]interface{}{
		{TIMESTAMP_MILLIS(0), TIMESTAMP_MILLIS(1), TIMESTAMP_MILLIS(2)},
		{TIMESTAMP_MILLIS(0), TIMESTAMP_MILLIS(0), TIMESTAMP_MILLIS(0)},
	}

	for _, data := range testData {
		res := ReadPlainTIMESTAMP_MILLIS(bytes.NewReader(WritePlainTIMESTAMP_MILLIS(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainTIMESTAMP_MILLIS err, %v", data)
		}
	}
}

func TestReadPlainTIMESTAMP_MICROS(t *testing.T) {
	testData := [][]interface{}{
		{TIMESTAMP_MICROS(0), TIMESTAMP_MICROS(1), TIMESTAMP_MICROS(2)},
		{TIMESTAMP_MICROS(0), TIMESTAMP_MICROS(0), TIMESTAMP_MICROS(0)},
	}

	for _, data := range testData {
		res := ReadPlainTIMESTAMP_MICROS(bytes.NewReader(WritePlainTIMESTAMP_MICROS(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainTIMESTAMP_MICROS err, %v", data)
		}
	}
}

func TestReadPlainFLOAT(t *testing.T) {
	testData := [][]interface{}{
		{FLOAT(0), FLOAT(1), FLOAT(2)},
		{FLOAT(0), FLOAT(0.1), FLOAT(0.2)},
	}

	for _, data := range testData {
		res := ReadPlainFLOAT(bytes.NewReader(WritePlainFLOAT(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainFLOAT err, %v", data)
		}
	}
}

func TestReadPlainDOUBLE(t *testing.T) {
	testData := [][]interface{}{
		{DOUBLE(0), DOUBLE(1), DOUBLE(2)},
		{DOUBLE(0), DOUBLE(0), DOUBLE(0)},
	}

	for _, data := range testData {
		res := ReadPlainDOUBLE(bytes.NewReader(WritePlainDOUBLE(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainDOUBLE err, %v", data)
		}
	}
}

func TestReadPlainINTERVAL(t *testing.T) {
	testData := [][]interface{}{
		{INTERVAL("0123456789ab"), INTERVAL("0123456789ab"), INTERVAL("0123456789ab")},
	}
	for _, data := range testData {
		res := ReadPlainINTERVAL(bytes.NewReader(WritePlainINTERVAL(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainINTERVAL err, %v", data)
		}
	}
}

func TestReadPlainDECIMAL(t *testing.T) {
	testData := [][]interface{}{
		{DECIMAL("0123456789ab"), DECIMAL("0123456789ab"), DECIMAL("0123456789ab")},
	}
	for _, data := range testData {
		res := ReadPlainDECIMAL(bytes.NewReader(WritePlainDECIMAL(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainDECIMAL err, %v", data)
		}
	}
}

func TestReadUnsignedVarInt(t *testing.T) {
	testData := []uint64{1, 2, 3, 11, 22, 33, 111, 222, 333, 0}
	for _, data := range testData {
		res := ReadUnsignedVarInt(bytes.NewReader(WriteUnsignedVarInt(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadUnsignedVarInt err, %v", data)
		}
	}
}

func TestReadRLEBitPackedHybrid(t *testing.T) {
	testData := [][]interface{}{
		[]interface{}{INT64(1), INT64(2), INT64(3), INT64(4)},
		[]interface{}{INT64(0), INT64(0), INT64(0), INT64(0), INT64(0)},
	}
	for _, data := range testData {
		maxVal := uint64(data[len(data)-1].(INT64))
		res := ReadRLEBitPackedHybrid(bytes.NewReader(WriteRLEBitPackedHybrid(data, int32(BitNum(maxVal)))), uint64(BitNum(maxVal)), 0)
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadRLEBitpackedHybrid err, %v", data)
		}
	}
}
