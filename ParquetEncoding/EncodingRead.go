package ParquetEncoding

import (
	"bytes"
	"encoding/binary"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/parquet"
)

func ReadPlain(bytesReader *bytes.Reader, dataType parquet.Type, cnt uint64, bitWidth uint64) []interface{} {
	if dataType == parquet.Type_BOOLEAN {
		return ReadPlainBOOLEAN(bytesReader, cnt)
	} else if dataType == parquet.Type_INT32 {
		return ReadPlainINT32(bytesReader, cnt)
	} else if dataType == parquet.Type_INT64 {
		return ReadPlainINT64(bytesReader, cnt)
	} else if dataType == parquet.Type_INT96 {
		return ReadPlainINT96(bytesReader, cnt)
	} else if dataType == parquet.Type_FLOAT {
		return ReadPlainFLOAT(bytesReader, cnt)
	} else if dataType == parquet.Type_DOUBLE {
		return ReadPlainDOUBLE(bytesReader, cnt)
	} else if dataType == parquet.Type_BYTE_ARRAY {
		return ReadPlainBYTE_ARRAY(bytesReader, cnt)
	} else if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		return ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader, cnt, bitWidth)
	} else {
		return nil
	}
}

func ReadPlainBOOLEAN(bytesReader *bytes.Reader, cnt uint64) []interface{} {
	res := make([]interface{}, cnt)
	resInt := ReadBitPacked(bytesReader, uint64(cnt<<1), 1)
	for i := 0; i < int(cnt); i++ {
		if resInt[i].(ParquetType.INT64) > 0 {
			res[i] = ParquetType.BOOLEAN(true)
		} else {
			res[i] = ParquetType.BOOLEAN(false)
		}
	}
	return res
}

func ReadPlainINT32(bytesReader *bytes.Reader, cnt uint64) []interface{} {
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		var cur ParquetType.INT32
		binary.Read(bytesReader, binary.LittleEndian, &cur)
		res[i] = cur
	}
	return res
}

func ReadPlainINT64(bytesReader *bytes.Reader, cnt uint64) []interface{} {
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		var cur ParquetType.INT64
		binary.Read(bytesReader, binary.LittleEndian, &cur)
		res[i] = cur
	}
	return res
}

func ReadPlainINT96(bytesReader *bytes.Reader, cnt uint64) []interface{} {
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		var cur [12]byte
		binary.Read(bytesReader, binary.LittleEndian, &cur)
		res[i] = ParquetType.INT96(cur[:12])
	}
	return res
}

func ReadPlainFLOAT(bytesReader *bytes.Reader, cnt uint64) []interface{} {
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		var cur ParquetType.FLOAT
		binary.Read(bytesReader, binary.LittleEndian, &cur)
		res[i] = cur
	}
	return res
}

func ReadPlainDOUBLE(bytesReader *bytes.Reader, cnt uint64) []interface{} {
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		var cur ParquetType.DOUBLE
		binary.Read(bytesReader, binary.LittleEndian, &cur)
		res[i] = cur
	}
	return res
}

func ReadPlainBYTE_ARRAY(bytesReader *bytes.Reader, cnt uint64) []interface{} {
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		buf := make([]byte, 4)
		bytesReader.Read(buf)
		ln := binary.LittleEndian.Uint32(buf)
		cur := make([]byte, ln)
		bytesReader.Read(cur)
		res[i] = ParquetType.BYTE_ARRAY(cur)
	}
	return res
}

func ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader *bytes.Reader, cnt uint64, fixedLength uint64) []interface{} {
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		cur := make([]byte, fixedLength)
		bytesReader.Read(cur)
		res[i] = ParquetType.FIXED_LEN_BYTE_ARRAY(cur)
	}
	return res
}

func ReadUnsignedVarInt(bytesReader *bytes.Reader) uint64 {
	var res uint64 = 0
	var shift uint64 = 0
	for {
		b, err := bytesReader.ReadByte()
		if err != nil {
			break
		}
		res |= ((uint64(b) & uint64(0x7F)) << uint64(shift))
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}
	return res
}

//RLE return res is []INT64
func ReadRLE(bytesReader *bytes.Reader, header uint64, bitWidth uint64) []interface{} {
	cnt := header >> 1
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	bytesReader.Read(data)
	for len(data) < 4 {
		data = append(data, byte(0))
	}
	val := ParquetType.INT64(binary.LittleEndian.Uint32(data))
	res := make([]interface{}, cnt)

	for i := 0; i < int(cnt); i++ {
		res[i] = val
	}
	return res
}

//return res is []INT64
func ReadBitPacked(bytesReader *bytes.Reader, header uint64, bitWidth uint64) []interface{} {
	numGroup := (header >> 1)
	cnt := numGroup * 8
	byteCnt := cnt * bitWidth / 8

	res := make([]interface{}, 0, cnt)

	if bitWidth == 0 {
		for i := 0; i < int(cnt); i++ {
			res = append(res, ParquetType.INT64(0))
		}
		return res
	}
	bytesBuf := make([]byte, byteCnt)
	bytesReader.Read(bytesBuf)

	i := 0
	var resCur uint64 = 0
	var resCurNeedBits uint64 = bitWidth
	var used uint64 = 0
	var left uint64 = 8 - used
	b := bytesBuf[i]
	for i < len(bytesBuf) {
		if left >= resCurNeedBits {
			resCur |= uint64(((uint64(b) >> uint64(used)) & ((1 << uint64(resCurNeedBits)) - 1)) << uint64(bitWidth-resCurNeedBits))
			res = append(res, ParquetType.INT64(resCur))
			left -= resCurNeedBits
			used += resCurNeedBits

			resCurNeedBits = bitWidth
			resCur = 0

			if left <= 0 && i+1 < len(bytesBuf) {
				i += 1
				b = bytesBuf[i]
				left = 8
				used = 0
			}

		} else {
			resCur |= uint64((uint64(b) >> uint64(used)) << uint64(bitWidth-resCurNeedBits))
			i += 1
			if i < len(bytesBuf) {
				b = bytesBuf[i]
			}
			resCurNeedBits -= left
			left = 8
			used = 0
		}
	}
	return res
}

//res is INT64
func ReadRLEBitPackedHybrid(bytesReader *bytes.Reader, bitWidth uint64, length uint64) []interface{} {
	res := make([]interface{}, 0)
	if length <= 0 {
		length = uint64(ReadPlainINT32(bytesReader, 1)[0].(ParquetType.INT32))
	}
	//log.Println("ReadRLEBitPackedHybrid length =", length)

	buf := make([]byte, length)
	bytesReader.Read(buf)
	newReader := bytes.NewReader(buf)
	for newReader.Len() > 0 {
		header := ReadUnsignedVarInt(newReader)
		if header&1 == 0 {
			res = append(res, ReadRLE(newReader, header, bitWidth)...)
		} else {
			res = append(res, ReadBitPacked(newReader, header, bitWidth)...)
		}
	}
	return res
}

//res is INT64
func ReadDeltaBinaryPackedINT(bytesReader *bytes.Reader) []interface{} {
	blockSize := ReadUnsignedVarInt(bytesReader)
	numMiniblocksInBlock := ReadUnsignedVarInt(bytesReader)
	numValues := ReadUnsignedVarInt(bytesReader)
	firstValueZigZag := ReadUnsignedVarInt(bytesReader)
	var firstValue int64 = int64(firstValueZigZag>>1) ^ (-int64(firstValueZigZag & 1))

	//log.Println("====", blockSize, numMiniblocksInBlock, numValues, firstValue)
	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res := make([]interface{}, 0)
	res = append(res, ParquetType.INT64(firstValue))
	for uint64(len(res)) < numValues {
		minDeltaZigZag := ReadUnsignedVarInt(bytesReader)
		var minDelta int64 = int64(minDeltaZigZag>>1) ^ (-int64(minDeltaZigZag & 1))
		var bitWidths = make([]uint64, numMiniblocksInBlock)
		for i := 0; uint64(i) < numMiniblocksInBlock; i++ {
			b, _ := bytesReader.ReadByte()
			bitWidths[i] = uint64(b)
		}
		for i := 0; uint64(i) < numMiniblocksInBlock; i++ {
			cur := ReadBitPacked(bytesReader, (numValuesInMiniBlock/8)<<1, bitWidths[i])
			for j := 0; j < len(cur); j++ {
				res = append(res, ParquetType.INT64(int64(res[len(res)-1].(ParquetType.INT64))+int64(cur[j].(ParquetType.INT64))+minDelta))
			}
		}
	}
	return res[:numValues]
}

func ReadDeltaLengthByteArray(bytesReader *bytes.Reader) []interface{} {
	lengths := ReadDeltaBinaryPackedINT(bytesReader)
	res := make([]interface{}, len(lengths))
	for i := 0; i < len(lengths); i++ {
		cur := ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader, 1, uint64(lengths[i].(ParquetType.INT64)))
		res[i] = ParquetType.BYTE_ARRAY(cur[0].(ParquetType.FIXED_LEN_BYTE_ARRAY))
	}
	return res
}

func ReadDeltaByteArray(bytesReader *bytes.Reader) []interface{} {
	prefixLengths := ReadDeltaBinaryPackedINT(bytesReader)
	suffixes := ReadDeltaLengthByteArray(bytesReader)
	res := make([]interface{}, len(prefixLengths))

	res[0] = suffixes[0]
	for i := 1; i < len(prefixLengths); i++ {
		prefixLength := prefixLengths[i].(ParquetType.INT64)
		prefix := res[i-1].(ParquetType.BYTE_ARRAY)[:prefixLength]
		suffix := suffixes[i].(ParquetType.BYTE_ARRAY)
		res[i] = prefix + suffix
	}
	return res
}
