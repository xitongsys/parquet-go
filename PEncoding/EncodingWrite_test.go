package PEncoding

import (
	"encoding/json"
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"testing"
)

func TestToInt64(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []int64
	}{
		{nums: []interface{}{int(1), int(2), int(3)}, expected: []int64{int64(1), int64(2), int64(3)}},
		{nums: []interface{}{true, false, true}, expected: []int64{int64(1), int64(0), int64(1)}},
		{nums: []interface{}{}, expected: []int64{}},
	}

	for _, data := range testData {
		res := ToInt64(data.nums)
		sb1, _ := json.Marshal(res)
		sb2, _ := json.Marshal(data.expected)
		s1, s2 := string(sb1), string(sb2)
		if s1 != s2 {
			t.Errorf("TestToInt64 Error, expected %v, get %v", s1, s2)
		}

	}
}

func TestWriteUnsignedVarInt(t *testing.T) {
	resBuf := make([]byte, 0)
	resBuf = append(resBuf, byte(0x00))
	resBuf = append(resBuf, byte(0x7F))
	resBuf = append(resBuf, byte(0x80), byte(0x01))
	resBuf = append(resBuf, byte(0x80), byte(0x40))
	resBuf = append(resBuf, byte(0xFF), byte(0x7F))
	resBuf = append(resBuf, byte(0x80), byte(0x80), byte(0x01))
	resBuf = append(resBuf, byte(0xFF), byte(0xFF), byte(0x7F))
	resBuf = append(resBuf, byte(0x80), byte(0x80), byte(0x80), byte(0x01))
	resBuf = append(resBuf, byte(0x80), byte(0x80), byte(0x80), byte(0x40))
	resBuf = append(resBuf, byte(0xFF), byte(0xFF), byte(0xFF), byte(0x7F))

	testNum := make([]uint32, 10)
	testNum[0] = 0x0
	testNum[1] = 0x7F
	testNum[2] = 0x80
	testNum[3] = 0x2000
	testNum[4] = 0x3FFF
	testNum[5] = 0x4000
	testNum[6] = 0x1FFFFF
	testNum[7] = 0x200000
	testNum[8] = 0x8000000
	testNum[9] = 0xFFFFFFF

	testRes := make([]byte, 0)
	for i := 0; i < len(testNum); i++ {
		tmpBuf := WriteUnsignedVarInt(uint64(testNum[i]))
		testRes = append(testRes, tmpBuf...)
	}

	if string(testRes) != string(resBuf) {
		t.Errorf("WriteUnsignedVarInt Error: Except: %v Get: %v", resBuf, testRes)
	}
}

func TestWriteRLE(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{INT64(0), INT64(0), INT64(0)}, []byte{byte(3 << 1)}},
	}

	for _, data := range testData {
		res := WriteRLE(data.nums, int32(BitNum(uint64(data.nums[0].(INT64)))))
		if string(res) != string(data.expected) {
			t.Errorf("WriteRLE error, expect %v, get %v", data.expected, res)
		}
	}
}

/*

func TestWriteBitPacked(t *testing.T) {
	testBuf := make([]interface{}, 8)
	for i := 0; i < len(testBuf); i++ {
		testBuf[i] = int32(i)
	}

	resBuf := make([]byte, 0)
	resBuf = append(resBuf, byte(0x3), byte(0x88), byte(0xC6), byte(0xFA))

	testRes := WriteBitPacked(testBuf, int64(BitNum(7)))

	if string(resBuf) != string(testRes) {
		t.Errorf("WriteBitPacked Error: Expect %v Get %v", resBuf, testRes)
	}

	testBuf = make([]interface{}, 8)
	for i := 0; i < len(testBuf); i++ {
		testBuf[i] = ((i % 2) == 0)
	}
	resBuf = make([]byte, 0)
	resBuf = append(resBuf, byte(0x3), byte(0x55))

	testRes = WriteBitPacked(testBuf, 1)

	if string(testRes) != string(resBuf) {
		t.Errorf("WriteBitPacked Error: Expect %v Get %v", resBuf, testRes)
	}
}

func TestWriteBitPackedDeprecated(t *testing.T) {
	vals := make([]interface{}, 0)
	vals = append(vals, 0, 1, 2, 3, 4, 5, 6, 7)
	fmt.Println(WriteBitPackedDeprecated(vals, 3))

}

*/
