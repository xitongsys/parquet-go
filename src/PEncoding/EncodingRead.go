package PEncoding

import (
	. "Common"
	"bytes"
	"encoding/binary"
	"log"
	"math"
	"parquet"
)

func ReadPlain(bytesReader *bytes.Reader, dataType parquet.Type, cnt int64) []Interface {
	if dataType == parquet.Type_BOOLEAN {
		res := ReadBitPacked(bytesReader, cnt<<1, 1)
		return res
	} else if dataType == parquet.Type_INT32 {
		resTmp := ReadPlainInt32(bytesReader, cnt)
		res := make([]Interface, len(resTmp))
		for i := 0; i < len(resTmp); i++ {
			res[i] = resTmp[i]
		}
		return res
	} else if dataType == parquet.Type_INT64 {
		resTmp := ReadPlainInt64(bytesReader, cnt)
		res := make([]Interface, len(resTmp))
		for i := 0; i < len(resTmp); i++ {
			res[i] = resTmp[i]
		}
		return res
	} else if dataType == parquet.Type_INT96 {
		resTmp := ReadPlainInt96(bytesReader, cnt)
		res := make([]Interface, len(resTmp))
		for i := 0; i < len(resTmp); i++ {
			res[i] = resTmp[i]
		}
		return res

	} else if dataType == parquet.Type_FLOAT {
		resTmp := ReadPlainFloat32(bytesReader, cnt)
		res := make([]Interface, len(resTmp))
		for i := 0; i < len(resTmp); i++ {
			res[i] = resTmp[i]
		}
		return res
	} else if dataType == parquet.Type_DOUBLE {
		resTmp := ReadPlainFloat64(bytesReader, cnt)
		res := make([]Interface, len(resTmp))
		for i := 0; i < len(resTmp); i++ {
			res[i] = resTmp[i]
		}
		return res
	} else if dataType == parquet.Type_BYTE_ARRAY {
		resTmp := ReadPlainByteArray(bytesReader, cnt)
		res := make([]Interface, len(resTmp))
		for i := 0; i < len(resTmp); i++ {
			res[i] = resTmp[i]
		}
		return res
	} else if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		resTmp := ReadPlainByteArrayFixed(bytesReader, cnt)
		res := make([]Interface, len(resTmp))
		for i := 0; i < len(resTmp); i++ {
			res[i] = resTmp[i]
		}
		return res
	} else {
		return nil
	}
}

func ReadPlainINT32(bytesReader *bytes.Reader, cnt int64) []INT32 {
	res := make([]INT32, cnt)
	for i := 0; i < int(cnt); i++ {
		binary.Read(bytesReader, binary.LittleEndian, &res[i])
	}
	return res
}

func ReadPlainINT64(bytesReader *bytes.Reader, cnt int64) []INT64 {
	res := make([]INT64, cnt)
	for i := 0; i < int(cnt); i++ {
		binary.Read(bytesReader, binary.LittleEndian, &res[i])
	}
	return res
}

func ReadPlainINT96(bytesReader *bytes.Reader, cnt int64) []INT96 {
	res := make([]INT96, cnt)
	for i := 0; i < int(cnt); i++ {
		binary.Read(bytesReader, binary.LittleEndian, &res[i])
	}
	return res
}

func ReadPlainFLOAT(bytesReader *bytes.Reader, cnt int64) []FLOAT {
	res := make([]FLOAT, cnt)
	for i := 0; i < int(cnt); i++ {
		binary.Read(bytesReader, binary.LittleEndian, &res[i])
	}
	return res
}

func ReadPlainDOUBLE(bytesReader *bytes.Reader, cnt int64) []DOUBLE {
	res := make([]DOUBLE, cnt)
	for i := 0; i < int(cnt); i++ {
		binary.Read(bytesReader, binary.LittleEndian, &res[i])
	}
	return res
}

func ReadPlainBYTE_ARRAY(bytesReader *bytes.Reader, cnt int64) []BYTE_ARRAY {
	res := make([]BYTE_ARRAY, cnt)
	for i := 0; i < int(cnt); i++ {
		buf := make([]byte, 4)
		bytesReader.Read(buf)
		ln := binary.LittleEndian.Uint32(buf)
		cur := make([]byte, ln)
		bytesReader.Read(cur)
		buf[i] = string(cur)
	}
	return res
}

func ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader *bytes.Reader, fixedLength int32, cnt int32) []FIXED_LEN_BYTE_ARRAY {
	res := make([]FIXED_LEN_BYTE_ARRAY, cnt)
	for i := 0; i < int(cnt); i++ {
		cur := make([]byte, fixedLength)
		bytesReader.Read(cur)
		res[i] = string(bytesReader)
	}
	return res
}

func ReadUnsignedVarInt(bytesReader *bytes.Reader) int32 {
	var res int32 = 0
	var shift int32 = 0
	for {
		b, err := bytesReader.ReadByte()
		if err != nil {
			break
		}
		res |= ((int32(b) & int32(0x7F)) << uint32(shift))
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}
	return res
}

//RLE return res is []INT64
func ReadRLE(bytesReader *bytes.Reader, header int32, bitWidth int32) []interface{} {
	cnt := header >> 1
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	bytesReader.Read(data)
	for len(data) < 4 {
		data = append(data, byte(0))
	}
	val := INT64(binary.LittleEndian.Uint32(data))
	res := make([]interface{}, cnt)

	for i := 0; i < int(cnt); i++ {
		res[i] = val
	}
	return res
}

//return res is []INT64
func ReadBitPacked(bytesReader *bytes.Reader, header int32, bitWidth int32) []interface{} {
	numGroup := (header >> 1)
	cnt := numGroup * 8
	byteCnt := cnt * bitWidth / 8
	res := make([]interface{}, 0)
	if bitWidth == 0 {
		for i := 0; i < int(cnt); i++ {
			res = append(res, 0)
		}
		return res
	}
	bytesBuf := make([]byte, byteCnt)
	bytesReader.Read(bytesBuf)

	i := 0
	var resCur int32 = 0
	var resCurNeedBits int32 = bitWidth
	var used int32 = 0
	var left int32 = 8 - used
	b := bytesBuf[i]
	for i < len(bytesBuf) {
		if left >= resCurNeedBits {
			resCur |= int32(((int(b) >> uint32(used)) & ((1 << uint32(resCurNeedBits)) - 1)) << uint32(bitWidth-resCurNeedBits))
			res = append(res, INT64(resCur))
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
			resCur |= int32((int(b) >> uint32(used)) << uint32(bitWidth-resCurNeedBits))
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
func ReadRLEBitPackedHybrid(bytesReader *bytes.Reader, bitWidth int32, length int32) []interface{} {
	res := make([]interface{}, 0)
	if length <= 0 {
		length = int32(ReadPlainINT32(bytesReader, 1)[0])
	}
	log.Println("ReadRLEBitPackedHybrid length =", length)

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

func ReadData(bytesReader *bytes.Reader, encoding parquet.Encoding, cnt int64, bitWidth int32) []Interface {
	res := make([]Interface, 0)
	if encoding == parquet.Encoding_RLE {
		for int32(len(res)) < cnt {
			resCur := ReadRLEBitPackedHybrid(bytesReader, bitWidth, 0)
			if resCur == nil || len(resCur) <= 0 {
				break
			}
			res = append(res, resCur...)
		}
	} else if encoding == parquet.Encoding_BIT_PACKED {
		log.Panicln("Encoding method not yet supported: ", encoding)
	} else if encoding == parquet.Encoding_PLAIN_DICTIONARY {
		log.Panicln("Encoding method not yet supported: ", encoding)
	} else {
		log.Panicln("Encoding method not yet supported: ", encoding)
	}
	return res
}
