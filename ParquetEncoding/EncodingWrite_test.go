package ParquetEncoding

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
		{[]interface{}{INT64(3)}, []byte{byte(1 << 1), byte(3)}},
		{[]interface{}{INT64(1), INT64(2), INT64(3), INT64(3)}, []byte{byte(1 << 1), byte(1), byte(1 << 1), byte(2), byte(2 << 1), byte(3)}},
	}

	for _, data := range testData {
		res := WriteRLE(data.nums, int32(BitNum(uint64(data.nums[len(data.nums)-1].(INT64)))))
		if string(res) != string(data.expected) {
			t.Errorf("WriteRLE error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteBitPacked(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{0, 0, 0, 0, 0, 0, 0, 0}, []byte{3}},
		{[]interface{}{0, 1, 2, 3, 4, 5, 6, 7}, []byte{3, 0x88, 0xC6, 0xFA}},
	}

	for _, data := range testData {
		res := WriteBitPacked(data.nums, int64(BitNum(uint64(data.nums[len(data.nums)-1].(int)))), true)
		if string(res) != string(data.expected) {
			t.Errorf("WriteRLE error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainBOOLEAN(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{BOOLEAN(true)}, []byte{1}},
		{[]interface{}{BOOLEAN(true), BOOLEAN(false)}, []byte{1}},
		{[]interface{}{BOOLEAN(true), BOOLEAN(false), BOOLEAN(false), BOOLEAN(true), BOOLEAN(false)}, []byte{9}},
	}

	for _, data := range testData {
		res := WritePlainBOOLEAN(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainBOOLEAN error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainINT32(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{INT32(0)}, []byte{0, 0, 0, 0}},
		{[]interface{}{INT32(0), INT32(1), INT32(2)}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainINT32(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainINT32 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainINT64(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{INT64(0)}, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{[]interface{}{INT64(0), INT64(1), INT64(2)}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainINT64(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainINT64 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainINT96(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{INT96([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{[]interface{}{
			INT96([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			INT96([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			INT96([]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},

			[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainINT96(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainINT96 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteINT_8(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{INT_8(0)}, []byte{0, 0, 0, 0}},
		{[]interface{}{INT_8(0), INT_8(1), INT_8(2)}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainINT_8(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainINT_8 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteINT_16(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{INT_16(0)}, []byte{0, 0, 0, 0}},
		{[]interface{}{INT_16(0), INT_16(1), INT_16(2)}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainINT_16(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainINT_16 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteINT_32(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{INT_32(0)}, []byte{0, 0, 0, 0}},
		{[]interface{}{INT_32(0), INT_32(1), INT_32(2)}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainINT_32(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainINT_32 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainINT_64(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{INT_64(0)}, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{[]interface{}{INT_64(0), INT_64(1), INT_64(2)}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainINT_64(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainINT_64 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteUINT_8(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{UINT_8(0)}, []byte{0, 0, 0, 0}},
		{[]interface{}{UINT_8(0), UINT_8(1), UINT_8(2)}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainUINT_8(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainUINT_8 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteUINT_16(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{UINT_16(0)}, []byte{0, 0, 0, 0}},
		{[]interface{}{UINT_16(0), UINT_16(1), UINT_16(2)}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainUINT_16(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainUINT_16 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteUINT_32(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{UINT_32(0)}, []byte{0, 0, 0, 0}},
		{[]interface{}{UINT_32(0), UINT_32(1), UINT_32(2)}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainUINT_32(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainUINT_32 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainUINT_64(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{UINT_64(0)}, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{[]interface{}{UINT_64(0), UINT_64(1), UINT_64(2)}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainUINT_64(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainUINT_64 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainUTF8(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{UTF8("a"), UTF8("abc")}, []byte{1, 0, 0, 0, 97, 3, 0, 0, 0, 97, 98, 99}},
	}

	for _, data := range testData {
		res := WritePlainUTF8(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainUTF8 error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainDATE(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{DATE(0)}, []byte{0, 0, 0, 0}},
		{[]interface{}{DATE(0), DATE(1), DATE(2)}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainDATE(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainDATE error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainTIME_MILLIS(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{TIME_MILLIS(0)}, []byte{0, 0, 0, 0}},
		{[]interface{}{TIME_MILLIS(0), TIME_MILLIS(1), TIME_MILLIS(2)}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainTIME_MILLIS(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainTIME_MILLIS error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainTIMESTAMP_MILLIS(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{TIMESTAMP_MILLIS(0)}, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{[]interface{}{TIMESTAMP_MILLIS(0), TIMESTAMP_MILLIS(1), TIMESTAMP_MILLIS(2)}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainTIMESTAMP_MILLIS(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WriteTIMESTAMP_MILLIS error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainTIMESTAMP_MICROS(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{TIMESTAMP_MICROS(0)}, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{[]interface{}{TIMESTAMP_MICROS(0), TIMESTAMP_MICROS(1), TIMESTAMP_MICROS(2)}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainTIMESTAMP_MICROS(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WriteTIMESTAMP_MICROS error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainINTERVAL(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{INTERVAL([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{[]interface{}{
			INTERVAL([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			INTERVAL([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			INTERVAL([]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},

			[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, data := range testData {
		res := WritePlainINTERVAL(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainINTERVAL error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainDECIMAL(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{DECIMAL("a"), DECIMAL("abc")}, []byte{1, 0, 0, 0, 97, 3, 0, 0, 0, 97, 98, 99}},
	}

	for _, data := range testData {
		res := WritePlainDECIMAL(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainDECIMAL error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainBYTE_ARRAY(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{BYTE_ARRAY("a"), BYTE_ARRAY("abc")}, []byte{1, 0, 0, 0, 97, 3, 0, 0, 0, 97, 98, 99}},
	}

	for _, data := range testData {
		res := WritePlainBYTE_ARRAY(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainBYTE_ARRAY error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWritePlainFIXED_LEN_BYTE_ARRAY(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{}, []byte{}},
		{[]interface{}{FIXED_LEN_BYTE_ARRAY("bca"), FIXED_LEN_BYTE_ARRAY("abc")}, []byte{98, 99, 97, 97, 98, 99}},
	}

	for _, data := range testData {
		res := WritePlainFIXED_LEN_BYTE_ARRAY(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WritePlainFIXED_LEN_BYTE_ARRAY error, expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteDeltaINT32(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{INT32(1), INT32(2), INT32(3), INT32(4), INT32(5)}, []byte{128, 1, 4, 5, 2, 2, 0, 0, 0, 0}},
		{
			[]interface{}{INT32(7), INT32(5), INT32(3), INT32(1), INT32(2), INT32(3), INT32(4), INT32(5)},
			[]byte{128, 1, 4, 8, 14, 3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0},
		},
	}

	for _, data := range testData {
		res := WriteDeltaINT32(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WriteDeltaINT32 error,expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteDeltaINT64(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{INT64(1), INT64(2), INT64(3), INT64(4), INT64(5)}, []byte{128, 1, 4, 5, 2, 2, 0, 0, 0, 0}},
		{
			[]interface{}{INT64(7), INT64(5), INT64(3), INT64(1), INT64(2), INT64(3), INT64(4), INT64(5)},
			[]byte{128, 1, 4, 8, 14, 3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0},
		},
	}

	for _, data := range testData {
		res := WriteDeltaINT64(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WriteDeltaINT64 error,expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteDeltaLengthByteArray(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{"Hello", "World", "Foobar", "ABCDEF"}, []byte{128, 1, 4, 4, 10, 0, 1, 0, 0, 0, 2, 0, 0, 0, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100, 70, 111, 111, 98, 97, 114, 65, 66, 67, 68, 69, 70}},
	}

	for _, data := range testData {
		res := WriteDeltaLengthByteArray(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WriteDeltaLengthByteArray error,expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteDeltaByteArray(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{"Hello", "World", "Foobar", "ABCDEF"}, []byte{128, 1, 4, 4, 0, 0, 0, 0, 0, 0, 128, 1, 4, 4, 10, 0, 1, 0, 0, 0, 2, 0, 0, 0, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100, 70, 111, 111, 98, 97, 114, 65, 66, 67, 68, 69, 70}},
	}

	for _, data := range testData {
		res := WriteDeltaByteArray(data.nums)
		if string(res) != string(data.expected) {
			t.Errorf("WriteDeltaByteArray error,expect %v, get %v", data.expected, res)
		}
	}
}

func TestWriteBitPackedDeprecated(t *testing.T) {
	testData := []struct {
		nums     []interface{}
		expected []byte
	}{
		{[]interface{}{1, 2, 3, 4}, []byte{41}},
	}

	for _, data := range testData {
		res := WriteBitPackedDeprecated(data.nums, 3)
		if string(res) != string(data.expected) {
			t.Errorf("WriteBitPackedDeprecated error,expect %v, get %v", data.expected, res)
		}
	}
}
