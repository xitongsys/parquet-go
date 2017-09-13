package PEncoding

import (
	. "Common"
	. "ParquetType"
	"bufio"
	"bytes"
	"encoding/binary"
	"reflect"
)

func ToInt64(nums []interface{}) []int64 { //convert bool/int values to int64 values
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

func WritePlain(src []interface{}) []byte {
	ln := len(src)
	if ln <= 0 {
		return []byte{}
	}

	dataType := reflect.TypeOf(src[0])

	if dataType == nil {
		return []byte{}
	}

	if dataType.Name() == "BOOLEAN" {
		srcTmp := make([]BOOLEAN, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(BOOLEAN)
		}
		return WritePlainBOOLEAN(srcTmp)

	} else if dataType.Name() == "INT32" {
		srcTmp := make([]INT32, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(INT32)
		}
		return WritePlainINT32(srcTmp)

	} else if dataType.Name() == "INT64" {
		srcTmp := make([]INT64, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(INT64)
		}
		return WritePlainINT64(srcTmp)
	} else if dataType.Name() == "INT96" {
		srcTmp := make([]INT96, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(INT96)
		}
		return WritePlainINT96(srcTmp)

	} else if dataType.Name() == "INT_8" {
		srcTmp := make([]INT_8, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(INT_8)
		}
		return WritePlainINT_8(srcTmp)

	} else if dataType.Name() == "INT_16" {
		srcTmp := make([]INT_16, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(INT_16)
		}
		return WritePlainINT_16(srcTmp)

	} else if dataType.Name() == "INT_32" {
		srcTmp := make([]INT_32, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(INT_32)
		}
		return WritePlainINT_32(srcTmp)

	} else if dataType.Name() == "INT_64" {
		srcTmp := make([]INT_64, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(INT_64)
		}
		return WritePlainINT_64(srcTmp)

	} else if dataType.Name() == "UINT_8" {
		srcTmp := make([]UINT_8, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(UINT_8)
		}
		return WritePlainUINT_8(srcTmp)

	} else if dataType.Name() == "UINT_16" {
		srcTmp := make([]UINT_16, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(UINT_16)
		}
		return WritePlainUINT_16(srcTmp)

	} else if dataType.Name() == "UINT_32" {
		srcTmp := make([]UINT_32, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(UINT_32)
		}
		return WritePlainUINT_32(srcTmp)

	} else if dataType.Name() == "UINT_64" {
		srcTmp := make([]UINT_64, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(UINT_64)
		}
		return WritePlainUINT_64(srcTmp)

	} else if dataType.Name() == "DATE" {
		srcTmp := make([]DATE, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(DATE)
		}
		return WritePlainDATE(srcTmp)

	} else if dataType.Name() == "TIME_MILLIS" {
		srcTmp := make([]TIME_MILLIS, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(TIME_MILLIS)
		}
		return WritePlainTIME_MILLIS(srcTmp)

	} else if dataType.Name() == "TIME_MICROS" {
		srcTmp := make([]TIME_MICROS, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(TIME_MICROS)
		}
		return WritePlainTIME_MICROS(srcTmp)

	} else if dataType.Name() == "TIMESTAMP_MILLIS" {
		srcTmp := make([]TIMESTAMP_MILLIS, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(TIMESTAMP_MILLIS)
		}
		return WritePlainTIMESTAMP_MILLIS(srcTmp)

	} else if dataType.Name() == "TIMESTAMP_MICROS" {
		srcTmp := make([]TIMESTAMP_MICROS, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(TIMESTAMP_MICROS)
		}
		return WritePlainTIMESTAMP_MICROS(srcTmp)

	} else if dataType.Name() == "INTERVAL" {
		srcTmp := make([]INTERVAL, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(INTERVAL)
		}
		return WritePlainINTERVAL(srcTmp)

	} else if dataType.Name() == "DECIMAL" {
		srcTmp := make([]DECIMAL, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(DECIMAL)
		}
		return WritePlainDECIMAL(srcTmp)

	} else if dataType.Name() == "FLOAT" {
		srcTmp := make([]FLOAT, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(FLOAT)
		}
		return WritePlainFLOAT(srcTmp)

	} else if dataType.Name() == "DOUBLE" {
		srcTmp := make([]DOUBLE, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(DOUBLE)
		}
		return WritePlainDOUBLE(srcTmp)

	} else if dataType.Name() == "BYTE_ARRAY" {
		srcTmp := make([]BYTE_ARRAY, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(BYTE_ARRAY)
		}
		return WritePlainBYTE_ARRAY(srcTmp)

	} else if dataType.Name() == "FIXED_LEN_BYTE_ARRAY" {
		srcTmp := make([]FIXED_LEN_BYTE_ARRAY, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(FIXED_LEN_BYTE_ARRAY)
		}
		return WritePlainFIXED_LEN_BYTE_ARRAY(srcTmp)

	} else if dataType.Name() == "UTF8" {
		srcTmp := make([]UTF8, ln)
		for i := 0; i < ln; i++ {
			srcTmp[i] = src[i].(UTF8)
		}
		return WritePlainUTF8(srcTmp)

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
			b := nums[i][j]
			binary.Write(bufWriter, binary.LittleEndian, &b)
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
	cnt := len(utf8s)
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
	for i := 0; i < len(dates); i++ {
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
	for i := 0; i < len(times); i++ {
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
	for i := 0; i < len(times); i++ {
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
	for i := 0; i < len(times); i++ {
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
	for i := 0; i < len(times); i++ {
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
	cnt := len(intervals)
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
	cnt := len(decimals)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(decimals[i]))
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
	bufTmp := make([]INT32, 1)
	bufTmp[0] = INT32(val)
	valBuf := WritePlainINT32(bufTmp)

	res := make([]byte, int64(len(headerBuf))+int64(byteNum))
	copy(res[0:], headerBuf)
	copy(res[len(headerBuf):], valBuf[0:byteNum])

	return res
}

func WriteBitPacked(vals []interface{}, bitWidth int64) []byte {
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
