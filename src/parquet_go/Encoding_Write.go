package parquet_go

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"reflect"
)

func BitNum(num uint64) uint64 { //the number of bits needed by the num; 0 needs 0, 1 need 1, 2 need 2, 3 need 2 ....
	var bitn int32 = 63
	for (bitn >= 0) && (((uint64(1) << uint32(bitn)) & num) == 0) {
		bitn--
	}
	return uint64(bitn + 1)
}

func ToInt64(nums []Interface) []int64 { //convert bool/int values to int64 values
	ln := len(nums)
	res := make([]int64, ln)
	tk := reflect.TypeOf(nums[0]).Kind()
	for i := 0; i < ln; i++ {
		if tk == reflect.Bool {
			if nums[i].(bool) {
				res[i] = 1
			} else {
				res[i] = 0
			}
		} else {
			res[i] = int64(reflect.ValueOf(nums[i]).Int())
		}

	}
	return res
}

func WritePlain(src []Interface) []byte {
	ln := len(src)
	if ln <= 0 {
		return []byte{}
	}

	dataType := reflect.TypeOf(src[0]).Kind()

	if dataType == reflect.Bool { //parquet.Type_BOOLEAN
		return WriteBitPacked(src, 1)
	} else if dataType == reflect.Int32 { //parquet.Type_INT32
		srcTmp := make([]int32, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(int32)
		}
		return WritePlainInt32(srcTmp)

	} else if dataType == reflect.Int64 { //parquet.Type_INT64
		srcTmp := make([]int64, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(int64)
		}
		return WritePlainInt64(srcTmp)

	} else if dataType == reflect.Float32 { // parquet.Type_FLOAT
		srcTmp := make([]float32, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(float32)
		}
		return WritePlainFloat32(srcTmp)

	} else if dataType == reflect.Float64 { // parquet.Type_DOUBLE
		srcTmp := make([]float64, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(float64)
		}
		return WritePlainFloat64(srcTmp)

	} else if dataType == reflect.String { // parquet.Type_BYTE_ARRAY
		srcTmp := make([][]byte, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = []byte(src[i].(string))
		}
		return WritePlainByteArray(srcTmp)

	} else {
		return nil
	}
}

func WritePlainInt32(nums []int32) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		binary.Write(bufWriter, binary.LittleEndian, &nums[i])
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*4)
	b.Read(res)
	return res
}

func WritePlainInt64(nums []int64) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		binary.Write(bufWriter, binary.LittleEndian, &nums[i])
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*8)
	b.Read(res)
	return res
}

func WritePlainFloat32(nums []float32) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		binary.Write(bufWriter, binary.LittleEndian, &nums[i])
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*4)
	b.Read(res)
	return res
}

func WritePlainFloat64(nums []float64) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		binary.Write(bufWriter, binary.LittleEndian, &nums[i])
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*8)
	b.Read(res)
	return res
}

func WritePlainByteArray(arrays [][]byte) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)

	var size uint32 = 0
	cnt := len(arrays)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(arrays[i]))
		binary.Write(bufWriter, binary.LittleEndian, &ln)
		bufWriter.Write(arrays[i])
		size += 4 + ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WriteUnsignedVarInt(num uint32) []byte {
	byteNum := (BitNum(uint64(num)) + 6) / 7
	if byteNum == 0 {
		return make([]byte,1)
	}
	res := make([]byte, byteNum)

	numTmp := num
	for i := 0; i < int(byteNum); i++ {
		res[i] = byte(numTmp & uint32(0x7F))
		res[i] = res[i] | byte(0x80)
		numTmp = numTmp >> 7
	}
	res[byteNum-1] &= byte(0x7F)
	return res
}

func WriteRLE(val int32, cnt int32, bitWidth int32) []byte {
	header := cnt << 1
	byteNum := (bitWidth + 7) / 8

	headerBuf := WriteUnsignedVarInt(uint32(header))
	bufTmp := make([]int32, 1)
	bufTmp[0] = val
	valBuf := WritePlainInt32(bufTmp)

	res := make([]byte, int64(len(headerBuf))+int64(byteNum))
	copy(res[0:], headerBuf)
	copy(res[len(headerBuf):], valBuf[0:byteNum])

	return res
}

func WriteBitPacked(vals []Interface, bitWidth int64) []byte {
	ln := len(vals)
	if ln <= 0 {
		return nil
	}
	valsInt := ToInt64(vals)

	header := ((ln/8)<<1 | 1)
	headerBuf := WriteUnsignedVarInt(uint32(header))

	valBuf := make([]byte, 0)

	i := 0
	var resCur int64 = 0
	var resCurNeedBits int64 = 8
	var used int64 = 0
	var left int64 = bitWidth - used
	val := int64(valsInt[i])
	for i < ln {
		if left >= resCurNeedBits {
			resCur |= ((val >> uint64(used)) & ((1 << uint64(resCurNeedBits)) - 1)) << uint64(8 - resCurNeedBits)
			valBuf = append(valBuf, byte(resCur))
			left -= resCurNeedBits
			used += resCurNeedBits

			resCurNeedBits = 8
			resCur = 0

			if left <= 0 && (i+1) < ln {
				i += 1
				val = int64(valsInt[i])
				left = bitWidth
				used = 0
			}

		} else {
			resCur |= (val >> uint64(used)) << uint64(8 - resCurNeedBits)
			i += 1

			if i < ln {
				val = int64(valsInt[i])
			}
			resCurNeedBits -= left

			left = bitWidth
			used = 0
		}
	}

	res := make([]byte, 0)
	res = append(res, headerBuf...)
	res = append(res, valBuf...)
	return res
}
