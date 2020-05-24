package encoding

import (
	"bytes"
	"fmt"
	"math/bits"
	"testing"

	"github.com/xitongsys/parquet-go/parquet"
)

func TestReadPlainBOOLEAN(t *testing.T) {
	testData := [][]interface{}{
		[]interface{}{(true)},
		[]interface{}{(false)},
		[]interface{}{(false), (false)},
		[]interface{}{(false), (true)},
	}

	for _, data := range testData {
		res, _ := ReadPlainBOOLEAN(bytes.NewReader(WritePlainBOOLEAN(data)), uint64(len(data)))
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
		{[]interface{}{int32(0)}, bytes.NewReader([]byte{0, 0, 0, 0})},
		{[]interface{}{int32(0), int32(1), int32(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0})},
	}

	for _, data := range testData {
		res, _ := ReadPlainINT32(data.byteReader, uint64(len(data.expected)))
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
		{[]interface{}{int64(0)}, bytes.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 0})},
		{[]interface{}{int64(0), int64(1), int64(2)}, bytes.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0})},
	}

	for _, data := range testData {
		res, _ := ReadPlainINT64(data.byteReader, uint64(len(data.expected)))
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data.expected) {
			t.Errorf("ReadPlainINT64 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestReadPlainBYTE_ARRAY(t *testing.T) {
	testData := [][]interface{}{
		{("hello"), ("world")},
		{("good"), (""), ("a"), ("b")},
	}

	for _, data := range testData {
		res, _ := ReadPlainBYTE_ARRAY(bytes.NewReader(WritePlainBYTE_ARRAY(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainBYTE_ARRAY err, %v", data)
		}
	}
}

func TestReadPlainFIXED_LEN_BYTE_ARRAY(t *testing.T) {
	testData := [][]interface{}{
		{("hello"), ("world")},
		{("a"), ("b"), ("c"), ("d")},
	}

	for _, data := range testData {
		res, _ := ReadPlainFIXED_LEN_BYTE_ARRAY(bytes.NewReader(WritePlainFIXED_LEN_BYTE_ARRAY(data)), uint64(len(data)), uint64(len(data[0].(string))))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainFIXED_LEN_BYTE_ARRAY err, %v", data)
		}
	}
}

func TestReadPlainFLOAT(t *testing.T) {
	testData := [][]interface{}{
		{float32(0), float32(1), float32(2)},
		{float32(0), float32(0.1), float32(0.2)},
	}

	for _, data := range testData {
		res, _ := ReadPlainFLOAT(bytes.NewReader(WritePlainFLOAT(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainFLOAT err, %v", data)
		}
	}
}

func TestReadPlainDOUBLE(t *testing.T) {
	testData := [][]interface{}{
		{float64(0), float64(1), float64(2)},
		{float64(0), float64(0), float64(0)},
	}

	for _, data := range testData {
		res, _ := ReadPlainDOUBLE(bytes.NewReader(WritePlainDOUBLE(data)), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainDOUBLE err, %v", data)
		}
	}
}

func TestReadUnsignedVarInt(t *testing.T) {
	testData := []uint64{1, 2, 3, 11, 22, 33, 111, 222, 333, 0}
	for _, data := range testData {
		res, _ := ReadUnsignedVarInt(bytes.NewReader(WriteUnsignedVarInt(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadUnsignedVarInt err, %v", data)
		}
	}
}

func TestReadRLEBitPackedHybrid(t *testing.T) {
	testData := [][]interface{}{
		[]interface{}{int64(1), int64(2), int64(3), int64(4)},
		[]interface{}{int64(0), int64(0), int64(0), int64(0), int64(0)},
	}
	for _, data := range testData {
		maxVal := uint64(data[len(data)-1].(int64))

		res, err := ReadRLEBitPackedHybrid(bytes.NewReader(WriteRLEBitPackedHybrid(data, int32(bits.Len64(maxVal)), parquet.Type_INT64)), uint64(bits.Len64(maxVal)), 0)
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadRLEBitpackedHybrid error, expect %v, get %v, err info:%v", data, res, err)
		}
	}
}

func TestReadDeltaBinaryPackedINT(t *testing.T) {
	testData := [][]interface{}{
		[]interface{}{int64(1), int64(2), int64(3), int64(4)},
		[]interface{}{int64(0), int64(0), int64(0), int64(0), int64(0)},
	}
	for _, data := range testData {
		res, _ := ReadDeltaBinaryPackedINT(bytes.NewReader(WriteDeltaINT64(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadRLEBitpackedHybrid error, expect %v, get %v", data, res)
		}
	}
}

func TestReadDeltaByteArray(t *testing.T) {
	testData := [][]interface{}{
		[]interface{}{"Hello", "world"},
	}
	for _, data := range testData {
		res, _ := ReadDeltaByteArray(bytes.NewReader(WriteDeltaByteArray(data)))
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
		res, _ := ReadDeltaLengthByteArray(bytes.NewReader(WriteDeltaLengthByteArray(data)))
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
		bitWidth := uint64(bits.Len(uint(data[ln-1].(int))))
		res, _ := ReadBitPacked(bytes.NewReader(WriteBitPacked(data, int64(bitWidth), false)), uint64(header), bitWidth)
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data) {

		}
	}
}
