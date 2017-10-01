package PEncoding

import (
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/ParquetType"
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
		return WritePlainBOOLEAN(src)
	} else if dataType.Name() == "INT32" {
		return WritePlainINT32(src)
	} else if dataType.Name() == "INT64" {
		return WritePlainINT64(src)
	} else if dataType.Name() == "INT96" {
		return WritePlainINT96(src)
	} else if dataType.Name() == "INT_8" {
		return WritePlainINT_8(src)
	} else if dataType.Name() == "INT_16" {
		return WritePlainINT_16(src)
	} else if dataType.Name() == "INT_32" {
		return WritePlainINT_32(src)
	} else if dataType.Name() == "INT_64" {
		return WritePlainINT_64(src)
	} else if dataType.Name() == "UINT_8" {
		return WritePlainUINT_8(src)
	} else if dataType.Name() == "UINT_16" {
		return WritePlainUINT_16(src)
	} else if dataType.Name() == "UINT_32" {
		return WritePlainUINT_32(src)
	} else if dataType.Name() == "UINT_64" {
		return WritePlainUINT_64(src)
	} else if dataType.Name() == "DATE" {
		return WritePlainDATE(src)
	} else if dataType.Name() == "TIME_MILLIS" {
		return WritePlainTIME_MILLIS(src)
	} else if dataType.Name() == "TIME_MICROS" {
		return WritePlainTIME_MICROS(src)
	} else if dataType.Name() == "TIMESTAMP_MILLIS" {
		return WritePlainTIMESTAMP_MILLIS(src)
	} else if dataType.Name() == "TIMESTAMP_MICROS" {
		return WritePlainTIMESTAMP_MICROS(src)
	} else if dataType.Name() == "INTERVAL" {
		return WritePlainINTERVAL(src)
	} else if dataType.Name() == "DECIMAL" {
		return WritePlainDECIMAL(src)
	} else if dataType.Name() == "FLOAT" {
		return WritePlainFLOAT(src)
	} else if dataType.Name() == "DOUBLE" {
		return WritePlainDOUBLE(src)
	} else if dataType.Name() == "BYTE_ARRAY" {
		return WritePlainBYTE_ARRAY(src)
	} else if dataType.Name() == "FIXED_LEN_BYTE_ARRAY" {
		return WritePlainFIXED_LEN_BYTE_ARRAY(src)
	} else if dataType.Name() == "UTF8" {
		return WritePlainUTF8(src)
	} else {
		return nil
	}
}

func WritePlainBOOLEAN(nums []interface{}) []byte {
	ln := len(nums)
	byteNum := (ln + 7) / 8
	res := make([]byte, byteNum)
	for i := 0; i < ln; i++ {
		if nums[i].(BOOLEAN) {
			res[i/8] = res[i/8] | (1 << uint32(i%8))
		}
	}
	return res
}

func WritePlainINT32(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num INT32 = nums[i].(INT32)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*4)
	b.Read(res)
	return res
}

func WritePlainINT64(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num INT64 = nums[i].(INT64)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*8)
	b.Read(res)
	return res
}

func WritePlainINT96(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		for j := 0; j < len(nums[i].(INT96)); j++ {
			b := nums[i].(INT96)[j]
			binary.Write(bufWriter, binary.LittleEndian, &b)
		}
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*12)
	b.Read(res)
	return res
}

func WritePlainINT_8(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num INT_8 = nums[i].(INT_8)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*4)
	b.Read(res)
	return res
}

func WritePlainINT_16(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num INT_16 = nums[i].(INT_16)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*4)
	b.Read(res)
	return res
}

func WritePlainINT_32(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num INT_32 = nums[i].(INT_32)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*4)
	b.Read(res)
	return res
}

func WritePlainINT_64(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num INT_64 = nums[i].(INT_64)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*8)
	b.Read(res)
	return res
}

func WritePlainUINT_8(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num UINT_8 = nums[i].(UINT_8)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*4)
	b.Read(res)
	return res
}

func WritePlainUINT_16(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num UINT_16 = nums[i].(UINT_16)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*4)
	b.Read(res)
	return res
}

func WritePlainUINT_32(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num UINT_32 = nums[i].(UINT_32)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*4)
	b.Read(res)
	return res
}

func WritePlainUINT_64(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num UINT_64 = nums[i].(UINT_64)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*8)
	b.Read(res)
	return res
}

func WritePlainFLOAT(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num FLOAT = nums[i].(FLOAT)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*4)
	b.Read(res)
	return res
}

func WritePlainDOUBLE(nums []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(nums); i++ {
		var num DOUBLE = nums[i].(DOUBLE)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(nums)*8)
	b.Read(res)
	return res
}

func WritePlainUTF8(utf8s []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	var size uint32 = 0
	cnt := len(utf8s)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(utf8s[i].(UTF8)))
		binary.Write(bufWriter, binary.LittleEndian, &ln)
		bufWriter.Write([]byte(utf8s[i].(UTF8)))
		size += 4 + ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WritePlainDATE(dates []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(dates); i++ {
		var num DATE = dates[i].(DATE)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(dates)*4)
	b.Read(res)
	return res
}

func WritePlainTIME_MILLIS(times []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(times); i++ {
		var num TIME_MILLIS = times[i].(TIME_MILLIS)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(times)*4)
	b.Read(res)
	return res
}

func WritePlainTIME_MICROS(times []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(times); i++ {
		var num TIME_MICROS = times[i].(TIME_MICROS)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(times)*8)
	b.Read(res)
	return res
}

func WritePlainTIMESTAMP_MILLIS(times []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(times); i++ {
		var num TIMESTAMP_MILLIS = times[i].(TIMESTAMP_MILLIS)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(times)*8)
	b.Read(res)
	return res
}

func WritePlainTIMESTAMP_MICROS(times []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	for i := 0; i < len(times); i++ {
		var num TIMESTAMP_MICROS = times[i].(TIMESTAMP_MICROS)
		binary.Write(bufWriter, binary.LittleEndian, &num)
	}
	bufWriter.Flush()
	res := make([]byte, len(times)*8)
	b.Read(res)
	return res
}

func WritePlainINTERVAL(intervals []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	var size uint32 = 0
	cnt := len(intervals)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(intervals[i].(INTERVAL)))
		bufWriter.Write([]byte(intervals[i].(INTERVAL)))
		size += ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WritePlainDECIMAL(decimals []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	var size uint32 = 0
	cnt := len(decimals)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(decimals[i].(DECIMAL)))
		binary.Write(bufWriter, binary.LittleEndian, &ln)
		bufWriter.Write([]byte(decimals[i].(DECIMAL)))
		size += 4 + ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WritePlainBYTE_ARRAY(arrays []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	var size uint32 = 0
	cnt := len(arrays)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(arrays[i].(BYTE_ARRAY)))
		binary.Write(bufWriter, binary.LittleEndian, &ln)
		bufWriter.Write([]byte(arrays[i].(BYTE_ARRAY)))
		size += 4 + ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WritePlainFIXED_LEN_BYTE_ARRAY(arrays []interface{}) []byte {
	var b bytes.Buffer
	bufWriter := bufio.NewWriter(&b)
	var size uint32 = 0
	cnt := len(arrays)
	for i := 0; i < int(cnt); i++ {
		ln := uint32(len(arrays[i].(FIXED_LEN_BYTE_ARRAY)))
		bufWriter.Write([]byte(arrays[i].(FIXED_LEN_BYTE_ARRAY)))
		size += ln
	}
	bufWriter.Flush()
	res := make([]byte, size)
	b.Read(res)
	return res
}

func WriteUnsignedVarInt(num uint64) []byte {
	byteNum := (BitNum(uint64(num)) + 6) / 7
	if byteNum == 0 {
		return make([]byte, 1)
	}
	res := make([]byte, byteNum)

	numTmp := num
	for i := 0; i < int(byteNum); i++ {
		res[i] = byte(numTmp & uint64(0x7F))
		res[i] = res[i] | byte(0x80)
		numTmp = numTmp >> 7
	}
	res[byteNum-1] &= byte(0x7F)
	return res
}

func WriteRLE(vals []interface{}, bitWidth int32) []byte {
	ln := len(vals)
	i := 0
	res := make([]byte, 0)
	for i < ln {
		j := i + 1
		for j < ln && vals[j] == vals[i] {
			j++
		}
		num := j - i
		header := num << 1
		byteNum := (bitWidth + 7) / 8
		headerBuf := WriteUnsignedVarInt(uint64(header))

		valBuf := WritePlainINT64([]interface{}{vals[i]})

		rleBuf := make([]byte, int64(len(headerBuf))+int64(byteNum))
		copy(rleBuf[0:], headerBuf)
		copy(rleBuf[len(headerBuf):], valBuf[0:byteNum])
		res = append(res, rleBuf...)
		i = j
	}
	return res
}

func WriteRLEBitPackedHybrid(vals []interface{}, bitWidths int32) []byte {
	rleBuf := WriteRLE(vals, bitWidths)
	res := make([]byte, 0)
	lenBuf := WritePlainINT32([]interface{}{INT32(len(rleBuf))})
	res = append(res, lenBuf...)
	res = append(res, rleBuf...)
	return res
}

func WriteBitPacked(vals []interface{}, bitWidth int64, ifHeader bool) []byte {
	ln := len(vals)
	if ln <= 0 {
		return nil
	}
	valsInt := ToInt64(vals)

	header := ((ln/8)<<1 | 1)
	headerBuf := WriteUnsignedVarInt(uint64(header))

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
	if ifHeader {
		res = append(res, headerBuf...)
	}
	res = append(res, valBuf...)
	return res
}

func WriteDeltaINT32(nums []interface{}) []byte {
	res := make([]byte, 0)
	var blockSize uint64 = 128
	var numMiniBlocksInBlock uint64 = 4
	var numValuesInMiniBlock uint64 = 32
	var totalNumValues uint64 = uint64(len(nums))

	num := int32(nums[0].(INT32))
	var firstValue uint64 = uint64((num >> 31) ^ (num << 1))

	res = append(res, WriteUnsignedVarInt(blockSize)...)
	res = append(res, WriteUnsignedVarInt(numMiniBlocksInBlock)...)
	res = append(res, WriteUnsignedVarInt(totalNumValues)...)
	res = append(res, WriteUnsignedVarInt(firstValue)...)

	i := 1
	for i < len(nums) {
		blockBuf := make([]interface{}, 0)
		var minDelta INT32 = 0x7FFFFFFF

		for i < len(nums) && uint64(len(blockBuf)) < blockSize {
			delta := INT32(nums[i].(INT32) - nums[i-1].(INT32))
			blockBuf = append(blockBuf, delta)
			if delta < minDelta {
				minDelta = delta
			}
			i++
		}

		for uint64(len(blockBuf)) < blockSize {
			blockBuf = append(blockBuf, minDelta)
		}

		bitWidths := make([]byte, numMiniBlocksInBlock)

		for j := 0; uint64(j) < numMiniBlocksInBlock; j++ {
			var maxValue INT32 = 0
			for k := uint64(j) * numValuesInMiniBlock; k < uint64(j+1)*numValuesInMiniBlock; k++ {
				blockBuf[k] = blockBuf[k].(INT32) - minDelta
				if blockBuf[k].(INT32) > maxValue {
					maxValue = blockBuf[k].(INT32)
				}
			}
			bitWidths[j] = byte(BitNum(uint64(maxValue)))
		}

		var minDeltaZigZag uint64 = uint64((minDelta >> 31) ^ (minDelta << 1))
		res = append(res, WriteUnsignedVarInt(minDeltaZigZag)...)
		res = append(res, bitWidths...)

		for j := 0; uint64(j) < numMiniBlocksInBlock; j++ {
			res = append(res, WriteBitPacked((blockBuf[uint64(j)*numValuesInMiniBlock:uint64(j+1)*numValuesInMiniBlock]), int64(bitWidths[j]), false)...)
		}

	}
	return res
}

func WriteDeltaINT64(nums []interface{}) []byte {
	res := make([]byte, 0)
	var blockSize uint64 = 128
	var numMiniBlocksInBlock uint64 = 4
	var numValuesInMiniBlock uint64 = 32
	var totalNumValues uint64 = uint64(len(nums))

	num := int64(nums[0].(INT64))
	var firstValue uint64 = uint64((num >> 63) ^ (num << 1))

	res = append(res, WriteUnsignedVarInt(blockSize)...)
	res = append(res, WriteUnsignedVarInt(numMiniBlocksInBlock)...)
	res = append(res, WriteUnsignedVarInt(totalNumValues)...)
	res = append(res, WriteUnsignedVarInt(firstValue)...)

	i := 1
	for i < len(nums) {
		blockBuf := make([]interface{}, 0)
		var minDelta INT64 = 0x7FFFFFFFFFFFFFFF

		for i < len(nums) && uint64(len(blockBuf)) < blockSize {
			delta := INT64(nums[i].(INT64) - nums[i-1].(INT64))
			blockBuf = append(blockBuf, delta)
			if delta < minDelta {
				minDelta = delta
			}
			i++
		}

		for uint64(len(blockBuf)) < blockSize {
			blockBuf = append(blockBuf, minDelta)
		}

		bitWidths := make([]byte, numMiniBlocksInBlock)

		for j := 0; uint64(j) < numMiniBlocksInBlock; j++ {
			var maxValue INT64 = 0
			for k := uint64(j) * numValuesInMiniBlock; k < uint64(j+1)*numValuesInMiniBlock; k++ {
				blockBuf[k] = blockBuf[k].(INT64) - minDelta
				if blockBuf[k].(INT64) > maxValue {
					maxValue = blockBuf[k].(INT64)
				}
			}
			bitWidths[j] = byte(BitNum(uint64(maxValue)))
		}

		var minDeltaZigZag uint64 = uint64((minDelta >> 63) ^ (minDelta << 1))
		res = append(res, WriteUnsignedVarInt(minDeltaZigZag)...)
		res = append(res, bitWidths...)

		for j := 0; uint64(j) < numMiniBlocksInBlock; j++ {
			res = append(res, WriteBitPacked((blockBuf[uint64(j)*numValuesInMiniBlock:uint64(j+1)*numValuesInMiniBlock]), int64(bitWidths[j]), false)...)
		}

	}
	return res
}

func WriteDeltaLengthByteArray(arrays []interface{}) []byte {
	ln := len(arrays)
	res := make([]byte, 0)
	lengthArray := make([]interface{}, ln)
	for i := 0; i < ln; i++ {
		array := reflect.ValueOf(arrays[i]).String()
		lengthArray[i] = INT32(len(array))
	}

	lengthBuf := WriteDeltaINT32(lengthArray)
	res = append(res, lengthBuf...)

	for i := 0; i < ln; i++ {
		array := reflect.ValueOf(arrays[i]).String()
		res = append(res, []byte(array)...)
	}
	return res
}

func WriteBitPackedDeprecated(vals []interface{}, bitWidth int64) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	valsInt := make([]uint64, ln)
	for i := 0; i < ln; i++ {
		valsInt[i] = uint64(reflect.ValueOf(vals[i]).Int())
	}

	res := make([]byte, 0)
	i := 0
	curByte := byte(0)
	var curNeed uint64 = 8
	var valBitLeft uint64 = uint64(bitWidth)
	var val uint64 = valsInt[0] << uint64(64-bitWidth)
	for i < ln {

		if valBitLeft > curNeed {
			var mask uint64 = ((1 << curNeed) - 1) << (64 - curNeed)

			curByte |= byte((val & mask) >> (64 - curNeed))
			val = val << curNeed

			valBitLeft -= curNeed
			res = append(res, curByte)
			curByte = byte(0)
			curNeed = 8

		} else {
			curByte |= byte(val >> (64 - curNeed))
			curNeed -= valBitLeft
			if curNeed == 0 {
				res = append(res, curByte)
				curByte = byte(0)
				curNeed = 8
			}

			valBitLeft = uint64(bitWidth)
			i++
			if i < ln {
				val = valsInt[i] << uint64(64-bitWidth)
			}
		}
	}
	return res
}

func WriteDeltaByteArray(arrays []interface{}) []byte {
	ln := len(arrays)
	if ln <= 0 {
		return []byte{}
	}

	prefixLengths := make([]interface{}, ln)
	suffixes := make([]interface{}, ln)
	prefixLengths[0] = INT32(0)
	suffixes[0] = arrays[0]

	for i := 1; i < ln; i++ {
		s1 := reflect.ValueOf(arrays[i-1]).String()
		s2 := reflect.ValueOf(arrays[i]).String()
		l1 := len(s1)
		l2 := len(s2)
		j := 0
		for j < l1 && j < l2 {
			if s1[j] != s2[j] {
				break
			}
			j++
		}
		prefixLengths[i] = INT32(j)
		suffixes[i] = BYTE_ARRAY(s2[j:])
	}

	prefixBuf := WriteDeltaINT32(prefixLengths)
	suffixBuf := WriteDeltaLengthByteArray(suffixes)

	res := make([]byte, 0)
	res = append(res, prefixBuf...)
	res = append(res, suffixBuf...)
	return res
}
