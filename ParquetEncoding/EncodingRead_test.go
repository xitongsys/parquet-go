package ParquetEncoding

import (
	"bytes"
	"fmt"
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"testing"
)

func TestReadPlainBOOLEAN(t *testing.T) {
	testData := [][]interface{}{
		[]interface{}{BOOLEAN(true)},
		[]interface{}{BOOLEAN(false)},
		[]interface{}{BOOLEAN(false), BOOLEAN(false)},
		[]interface{}{BOOLEAN(false), BOOLEAN(true)},
	}

	for _, data := range testData {
		res := ReadPlainBOOLEAN(bytes.NewReader(WritePlainBOOLEAN(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainBOOLEAN err, expect %v, get %v", data, res)
		}
	}
}

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

func TestReadDeltaBinaryPackedINT(t *testing.T) {
	testData := [][]interface{}{
		[]interface{}{INT64(1), INT64(2), INT64(3), INT64(4)},
		[]interface{}{INT64(0), INT64(0), INT64(0), INT64(0), INT64(0)},
	}
	for _, data := range testData {
		res := ReadDeltaBinaryPackedINT(bytes.NewReader(WriteDeltaINT64(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadRLEBitpackedHybrid err, expect %v, get %v", data, res)
		}
	}
}

func TestReadDeltaByteArray(t *testing.T) {
	testData := [][]interface{}{
		[]interface{}{"Hello", "world"},
	}
	for _, data := range testData {
		res := ReadDeltaByteArray(bytes.NewReader(WriteDeltaByteArray(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadDeltaByteArray err, expect %v, get %v", data, res)
		}
	}
}

func TestReadLengthDeltaByteArray(t *testing.T) {
	testData := [][]interface{}{
		[]interface{}{"Hello", "world"},
	}
	for _, data := range testData {
		res := ReadDeltaLengthByteArray(bytes.NewReader(WriteDeltaLengthByteArray(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadDeltaByteArray err, expect %v, get %v", data, res)
		}
	}
}

func TestReadBitPacked(t *testing.T) {
	testData := [][]interface{}{
		[]interface{}{1, 2, 3, 4, 5, 6, 7, 8},
		[]interface{}{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	for _, data := range testData {
		ln := len(data)
		header := ((ln/8)<<1 | 1)
		bitWidth := BitNum(uint64(data[ln-1].(int)))
		res := ReadBitPacked(bytes.NewReader(WriteBitPacked(data, int64(bitWidth), false)), uint64(header), bitWidth)
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data) {

		}
	}
}
