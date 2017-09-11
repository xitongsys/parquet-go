package Encoding

import (
	. "Common"
	"testing"
)

func TestBitNum(t *testing.T) {
	testNum := make([]uint64, 4)
	resNum := make([]uint64, 4)

	testNum[0] = 0
	resNum[0] = 0
	testNum[1] = 1
	resNum[1] = 1
	testNum[2] = 5
	resNum[2] = 3
	testNum[3] = 1023
	resNum[3] = 10

	for i := 0; i < len(testNum); i++ {
		if resNum[i] != BitNum(testNum[i]) {
			t.Error("WidthFromMaxInt Error Case Num=", testNum[i])
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
		tmpBuf := WriteUnsignedVarInt(testNum[i])
		testRes = append(testRes, tmpBuf...)
	}

	if string(testRes) != string(resBuf) {
		t.Errorf("WriteUnsignedVarInt Error: Except: %v Get: %v", resBuf, testRes)
	}
}

func TestWriteRLE(t *testing.T) {
	resBuf := make([]byte, 0)
	resBuf = append(resBuf, byte(0x2<<1))

	testRes := WriteRLE(0, 2, 0)
	if string(resBuf) != string(testRes) {
		t.Errorf("WriteRLE Error: Expect %v Get %v", resBuf, testRes)
	}

	resBuf = make([]byte, 0)
	resBuf = append(resBuf, byte(0x2<<1), byte(0x2))
	testRes = WriteRLE(2, 2, WidthFromMaxInt(2))
	if string(resBuf) != string(testRes) {
		t.Errorf("WriteRLE Error: Expect %v Get %v", resBuf, testRes)
	}
}

func TestWriteBitPacked(t *testing.T) {
	testBuf := make([]interface{}, 8)
	for i := 0; i < len(testBuf); i++ {
		testBuf[i] = int32(i)
	}

	resBuf := make([]byte, 0)
	resBuf = append(resBuf, byte(0x3), byte(0x88), byte(0xC6), byte(0xFA))

	testRes := WriteBitPacked(testBuf, int64(WidthFromMaxInt(7)))

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

func TestWritePlainINT96(t *testing.T) {

}
