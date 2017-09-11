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

	dataType := reflect.TypeOf(src[0])

	if dataType.Kind() == reflect.Bool { //parquet.Type_BOOLEAN
		//		return WriteBitPacked(src, 1)
		srcTmp := make([]bool, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(bool)
		}
		return WriteBoolean(srcTmp)

	} else if dataType.Kind() == reflect.Int32 { //parquet.Type_INT32
		srcTmp := make([]int32, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(int32)
		}
		return WritePlainInt32(srcTmp)

	} else if dataType.Kind() == reflect.Int64 { //parquet.Type_INT64
		srcTmp := make([]int64, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(int64)
		}
		return WritePlainInt64(srcTmp)

	} else if dataType.Kind() == reflect.Float32 { // parquet.Type_FLOAT
		srcTmp := make([]float32, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(float32)
		}
		return WritePlainFloat32(srcTmp)

	} else if dataType.Kind() == reflect.Float64 { // parquet.Type_DOUBLE
		srcTmp := make([]float64, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(float64)
		}
		return WritePlainFloat64(srcTmp)

	} else if dataType.Kind() == reflect.String { // parquet.Type_BYTE_ARRAY
		srcTmp := make([][]byte, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = []byte(src[i].(string))
		}
		return WritePlainByteArray(srcTmp)

	} else if dataType.Kind() == reflect.Slice && dataType.Name() == "INT96" { //parquet.INT96
		srcTmp := make([]INT96, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(INT96)
		}
		return WritePlainInt96(srcTmp)

	} else {
		return nil
	}
}

func WritePlainBOOLEAN(nums []BOOLEAN) []byte {
	ln := len(nums)
	byteNum := (ln + 7) / 8
	res := make([]byte, byteNum)
	for i := 0; i < ln; i++ {
		if nums[i] {
			res[i/8] = res[i/8] | (1 << uint32(i%8))
		}
	}
	return res
}

func WritePlainINT32(nums []INT32) []byte {
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

func WritePlainINT64(nums []INT64) []byte {
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

func WritePlainINT96(nums []INT96) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		for j := 0; j < len(nums[i]); j++ {
			binary.Write(bufWriter, binary.LittleEndian, &nums[i][j])
		}
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*12)
	b.Read(res)
	return res
}

func WritePlainINT_8(nums []INT_8) []byte {
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

func WritePlainINT_16(nums []INT_16) []byte {
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

func WritePlainINT_32(nums []INT_32) []byte {
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

func WritePlainINT_64(nums []INT_64) []byte {
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

func WritePlainUINT_8(nums []UINT_8) []byte {
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

func WritePlainUINT_16(nums []UINT_16) []byte {
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

func WritePlainUINT_32(nums []UINT_32) []byte {
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

func WritePlainUINT_64(nums []UINT_64) []byte {
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

func WritePlainFLOAT(nums []FLOAT) []byte {
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

func WritePlainDOUBLE(nums []DOUBLE) []byte {
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

func WritePlainUTF8(utf8s []UTF8) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	var size uint32 = 0
	cnt := len(arrays)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(utf8s[i]))
		binary.Write(bufWriter, binary.LittleEndian, &ln)
		bufWriter.Write([]byte(utf8s[i]))
		size += 4 + ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WritePlainDATE(dates []DATE) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		binary.Write(bufWriter, binary.LittleEndian, &dates[i])
	}
	bufWriter.Flush()
	res := make([]byte, len(dates)*4)
	b.Read(res)
	return res
}

func WritePlainTIME_MILLIS(times []TIME_MILLIS) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		binary.Write(bufWriter, binary.LittleEndian, &times[i])
	}
	bufWriter.Flush()
	res := make([]byte, len(times)*4)
	b.Read(res)
	return res
}

func WritePlainTIME_MICROS(times []TIME_MICROS) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		binary.Write(bufWriter, binary.LittleEndian, &times[i])
	}
	bufWriter.Flush()
	res := make([]byte, len(times)*8)
	b.Read(res)
	return res
}

func WritePlainTIMESTAMP_MILLIS(times []TIMESTAMP_MILLIS) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		binary.Write(bufWriter, binary.LittleEndian, &times[i])
	}
	bufWriter.Flush()
	res := make([]byte, len(times)*8)
	b.Read(res)
	return res
}

func WritePlainTIMESTAMP_MICROS(times []TIMESTAMP_MICROS) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		binary.Write(bufWriter, binary.LittleEndian, &times[i])
	}
	bufWriter.Flush()
	res := make([]byte, len(times)*8)
	b.Read(res)
	return res
}

func WritePlainINTERVAL(intervals []INTERVAL) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	var size uint32 = 0
	cnt := len(arrays)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(intervals[i]))
		bufWriter.Write([]byte(intervals[i]))
		size += ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WritePlainDECIMAL(decimals []DECIMAL) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	var size uint32 = 0
	cnt := len(arrays)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(arrays[i]))
		binary.Write(bufWriter, binary.LittleEndian, &ln)
		bufWriter.Write([]byte(decimals[i]))
		size += 4 + ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WritePlainBYTE_ARRAY(arrays []BYTE_ARRAY) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	var size uint32 = 0
	cnt := len(arrays)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(arrays[i]))
		binary.Write(bufWriter, binary.LittleEndian, &ln)
		bufWriter.Write([]byte(arrays[i]))
		size += 4 + ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WritePlainFIXED_LEN_BYTE_ARRAY(arrays []FIXED_LEN_BYTE_ARRAY) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	var size uint32 = 0
	cnt := len(arrays)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(arrays[i]))
		bufWriter.Write([]byte(arrays[i]))
		size += ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WriteUnsignedVarInt(num uint32) []byte {
	byteNum := (BitNum(uint64(num)) + 6) / 7
	if byteNum == 0 {
		return make([]byte, 1)
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
			resCur |= ((val >> uint64(used)) & ((1 << uint64(resCurNeedBits)) - 1)) << uint64(8-resCurNeedBits)
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
			resCur |= (val >> uint64(used)) << uint64(8-resCurNeedBits)
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
