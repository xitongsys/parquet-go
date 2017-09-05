package parquet_go

import (
	"bytes"
	"testing"
)

func Test_WidthFromMaxInt(t *testing.T) {
	testNum := make([]int32, 4)
	resNum := make([]int32, 4)

	testNum[0] = 0
	resNum[0] = 0
	testNum[1] = 1
	resNum[1] = 1
	testNum[2] = 5
	resNum[2] = 3
	testNum[3] = 1023
	resNum[3] = 10

	for i := 0; i < len(testNum); i++ {
		if resNum[i] != WidthFromMaxInt(testNum[i]) {
			t.Error("WidthFromMaxInt Error Case Num=", testNum[i])
		}
	}
}

func Test_ReadUnsignedVarInt(t *testing.T) {
	testBuf := make([]byte, 0)
	testBuf = append(testBuf, byte(0x00))
	testBuf = append(testBuf, byte(0x7F))
	testBuf = append(testBuf, byte(0x80), byte(0x01))
	testBuf = append(testBuf, byte(0x80), byte(0x40))
	testBuf = append(testBuf, byte(0xFF), byte(0x7F))
	testBuf = append(testBuf, byte(0x80), byte(0x80), byte(0x01))
	testBuf = append(testBuf, byte(0xFF), byte(0xFF), byte(0x7F))
	testBuf = append(testBuf, byte(0x80), byte(0x80), byte(0x80), byte(0x01))
	testBuf = append(testBuf, byte(0x80), byte(0x80), byte(0x80), byte(0x40))
	testBuf = append(testBuf, byte(0xFF), byte(0xFF), byte(0xFF), byte(0x7F))

	resNum := make([]int32, 10)
	resNum[0] = 0x0
	resNum[1] = 0x7F
	resNum[2] = 0x80
	resNum[3] = 0x2000
	resNum[4] = 0x3FFF
	resNum[5] = 0x4000
	resNum[6] = 0x1FFFFF
	resNum[7] = 0x200000
	resNum[8] = 0x8000000
	resNum[9] = 0xFFFFFFF

	reader := bytes.NewReader(testBuf)

	for i := 0; i < len(resNum); i++ {
		res := ReadUnsignedVarInt(reader)
		if res != resNum[i] {
			t.Errorf("ReadUnsignedVarInt Error: Except:0x%x, Get:0x%x", resNum[i], res)
		}
	}
}

func Test_ReadRLE(t *testing.T) {
	testBuf := make([]byte, 0)
	testBuf = append(testBuf, byte(0x2))

	reader := bytes.NewReader(testBuf)
	res := ReadRLE(reader, 2<<1, WidthFromMaxInt(0))
	if len(res) != 2 {
		t.Errorf("ReadRLE Error: Expect: [0 0], Get: %v", res)
	}
	for i := 0; i < len(res); i++ {
		if res[i].(int64) != 0 {
			t.Errorf("ReadRLE Error: Expect: [0 0], Get: %v", res)
		}
	}

	res = ReadRLE(reader, 2<<1, WidthFromMaxInt(2))
	if len(res) != 2 {
		t.Errorf("ReadRLE Error: Expect: [2 2], Get: %v", res)
	}
	for i := 0; i < len(res); i++ {
		if res[i].(int64) != 2 {
			t.Errorf("ReadRLE Error: Expect: [2 2], Get: %v", res)
		}
	}
}

func Test_ReadBitPacked(t *testing.T) {
	testBuf := make([]byte, 0)
	testBuf = append(testBuf, byte(0x88), byte(0xc6), byte(0xfa))

	reader := bytes.NewReader(testBuf)
	res := ReadBitPacked(reader, 1<<1, WidthFromMaxInt(7))

	if len(res) != 8 {
		t.Errorf("ReadRLE Error: Expect: [0 1 2 3 4 5 6 7], Get: %v", res)
	}
	for i := 0; i < 8; i++ {
		if int64(i) != res[i].(int64) {
			t.Errorf("ReadRLE Error: Expect: [0 1 2 3 4 5 6 7], Get: %v", res)
			break
		}
	}
}

func Test_ReadPlainInt96(t *testing.T) {
	testBuf := make([]byte, 0)
	testBuf = append(testBuf,
		byte(0xFA), byte(0xFA), byte(0xFA), byte(0x7A),
		byte(0xFA), byte(0xFA), byte(0xFA), byte(0x7A),
		byte(0xFA), byte(0xFA), byte(0xFA), byte(0x7A),

		byte(0xFB), byte(0xFB), byte(0xFB), byte(0x8B),
		byte(0xFB), byte(0xFB), byte(0xFB), byte(0x8B),
		byte(0xFB), byte(0xFB), byte(0xFB), byte(0x8B))

	reader := bytes.NewReader(testBuf)
	testRes := ReadPlainInt96(reader, 2)

	res := make([][3]int32, 2)
	res[0][0] = 0x7AFAFAFA
	res[0][1] = 0x7AFAFAFA
	res[0][2] = 0x7AFAFAFA

	res[1][0] = 0x7BFBFBFB
	res[1][1] = 0x7BFBFBFB
	res[1][2] = 0x7BFBFBFB

	for i:=0; i<2; i++ {
		for j:=0; j<3; j++ {
			if res[i][j] != testRes[i][j] {
				t.Errorf("ReadPlainInt96 Error")
				return
			}
		}
	}
	
}
