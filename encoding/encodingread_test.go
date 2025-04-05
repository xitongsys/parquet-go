package encoding

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"testing"
	"unsafe"

	"github.com/hangxie/parquet-go/parquet"
)

func TestReadPlainBOOLEAN(t *testing.T) {
	testData := [][]interface{}{
		{(true)},
		{(false)},
		{(false), (false)},
		{(false), (true)},
	}

	for _, data := range testData {
		buf, err := WritePlainBOOLEAN(data)
		if err != nil {
			t.Errorf("WritePlainBOOLEAN err, %v", err)
			continue
		}
		res, _ := ReadPlainBOOLEAN(bytes.NewReader(buf), uint64(len(data)))
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
		buf, err := WritePlainBYTE_ARRAY(data)
		if err != nil {
			t.Errorf("WritePlainBYTE_ARRAY err, %v", err)
			continue
		}
		res, _ := ReadPlainBYTE_ARRAY(bytes.NewReader(buf), uint64(len(data)))
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
		buf, err := WritePlainFIXED_LEN_BYTE_ARRAY(data)
		if err != nil {
			t.Errorf("WritePlainFIXED_LEN_BYTE_ARRAY err, %v", err)
			continue
		}
		res, _ := ReadPlainFIXED_LEN_BYTE_ARRAY(bytes.NewReader(buf), uint64(len(data)), uint64(len(data[0].(string))))
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
		buf, err := WritePlainFLOAT(data)
		if err != nil {
			t.Errorf("WritePlainFLOAT err, %v", err)
			continue
		}
		res, _ := ReadPlainFLOAT(bytes.NewReader(buf), uint64(len(data)))
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
		buf, err := WritePlainDOUBLE(data)
		if err != nil {
			t.Errorf("WritePlainDOUBLE err, %v", err)
			continue
		}
		res, _ := ReadPlainDOUBLE(bytes.NewReader(buf), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainDOUBLE err, %v", data)
		}
	}
}

func TestReadUnsignedVarInt(t *testing.T) {
	i32 := int32(-1570499385)

	testData := []uint64{1, 2, 3, 11, 1570499385, uint64(i32), 111, 222, 333, 0}
	for _, data := range testData {
		res, _ := ReadUnsignedVarInt(bytes.NewReader(WriteUnsignedVarInt(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadUnsignedVarInt err, %v", data)
		}
	}
}

func TestReadRLEBitPackedHybrid(t *testing.T) {
	testData := [][]interface{}{
		{int64(1), int64(2), int64(3), int64(4)},
		{int64(0), int64(0), int64(0), int64(0), int64(0)},
	}
	for _, data := range testData {
		maxVal := uint64(data[len(data)-1].(int64))
		buf, err := WriteRLEBitPackedHybrid(data, int32(bits.Len64(maxVal)), parquet.Type_INT64)
		if err != nil {
			t.Errorf("WriteRLEBitPackedHybrid err, %v", err)
			continue
		}
		res, err := ReadRLEBitPackedHybrid(bytes.NewReader(buf), uint64(bits.Len64(maxVal)), 0)
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadRLEBitpackedHybrid error, expect %v, get %v, err info:%v", data, res, err)
		}
	}
}

func TestReadDeltaBinaryPackedINT(t *testing.T) {
	testData := [][]interface{}{
		{int64(1), int64(2), int64(3), int64(4)},
		{int64(math.MaxInt64), int64(math.MinInt64), int64(-15654523568543623), int64(4354365463543632), int64(0)},
	}

	for _, data := range testData {
		fmt.Println(data)
		res, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(WriteDeltaINT64(data)))
		if err != nil {
			t.Error(err)
		}

		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadDeltaBinaryPackedINT64 error, expect %v, get %v", data, res)
		}
	}
}

func TestReadDeltaINT32(t *testing.T) {
	bInt32 := func(n int32) string { return strconv.FormatUint(uint64(*(*uint32)(unsafe.Pointer(&n))), 2) }
	buInt64 := func(n uint64) string { return strconv.FormatUint(n, 2) }
	testData := []int32{1, -1570499385, 3, -11, 1570499385, 111, 222, 333, 0}
	for _, data := range testData {
		fmt.Println("SRC32:", bInt32(data), data)
		u64 := uint64((data >> 31) ^ (data << 1))
		fmt.Println("SRC64:", buInt64(u64))
		resZigZag, err := ReadUnsignedVarInt(bytes.NewReader(WriteUnsignedVarInt(u64)))
		if err != nil {
			t.Error(err)
		}
		res32 := int32(resZigZag)
		var res int32 = int32(uint32(res32)>>1) ^ -(res32 & 1)
		fmt.Println("RES32:", bInt32(res), res)
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadUnsignedVarInt err, %v", data)
		}
	}
}

func TestReadDeltaBinaryPackedINT32(t *testing.T) {
	testData := [][]interface{}{
		{int32(1), int32(2), int32(3), int32(4)},
		{int32(-1570499385), int32(-1570499385), int32(-1570499386), int32(-1570499388), int32(-1570499385)},
	}

	for _, data := range testData {
		fmt.Println("source:", data)

		res, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(WriteDeltaINT32(data)))
		if err != nil {
			t.Error(err)
		}
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadDeltaBinaryPackedINT32 error, expect %v, get %v", data, res)
		}
	}
}

func TestReadDeltaByteArray(t *testing.T) {
	testData := [][]interface{}{
		{"Hello", "world"},
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
		{"Hello", "world"},
	}
	for _, data := range testData {
		res, _ := ReadDeltaLengthByteArray(bytes.NewReader(WriteDeltaLengthByteArray(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadDeltaLengthByteArray err, expect %v, get %v", data, res)
		}
	}
}

func TestReadBitPacked(t *testing.T) {
	testData := [][]interface{}{
		{1, 2, 3, 4, 5, 6, 7, 8},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	for _, data := range testData {
		ln := len(data)
		header := ((ln/8)<<1 | 1)
		bitWidth := uint64(bits.Len(uint(data[ln-1].(int))))
		res, _ := ReadBitPacked(bytes.NewReader(WriteBitPacked(data, int64(bitWidth), false)), uint64(header), bitWidth)
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data) {
			t.Errorf("ReadBitPacked err, expect %v, get %v", data, res)
		}
	}
}
