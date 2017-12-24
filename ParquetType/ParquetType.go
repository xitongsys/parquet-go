package ParquetType

import (
	"fmt"
	"github.com/xitongsys/parquet-go/parquet"
	"math/big"
)

//base type
type BOOLEAN bool
type INT32 int32
type INT64 int64
type INT96 string // length=96
type FLOAT float32
type DOUBLE float64
type BYTE_ARRAY string
type FIXED_LEN_BYTE_ARRAY string

//logical type
type UTF8 string
type INT_8 int32
type INT_16 int32
type INT_32 int32
type INT_64 int64
type UINT_8 uint32
type UINT_16 uint32
type UINT_32 uint32
type UINT_64 uint64
type DATE int32
type TIME_MILLIS int32
type TIME_MICROS int64
type TIMESTAMP_MILLIS int64
type TIMESTAMP_MICROS int64
type INTERVAL string // length=12
type DECIMAL string

func TypeNameToParquetType(name string, baseName string) (*parquet.Type, *parquet.ConvertedType) {
	if name == "BOOLEAN" {
		return parquet.TypePtr(parquet.Type_BOOLEAN), nil
	} else if name == "INT32" {
		return parquet.TypePtr(parquet.Type_INT32), nil
	} else if name == "INT64" {
		return parquet.TypePtr(parquet.Type_INT64), nil
	} else if name == "INT96" {
		return parquet.TypePtr(parquet.Type_INT96), nil
	} else if name == "FLOAT" {
		return parquet.TypePtr(parquet.Type_FLOAT), nil
	} else if name == "DOUBLE" {
		return parquet.TypePtr(parquet.Type_DOUBLE), nil
	} else if name == "BYTE_ARRAY" {
		return parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil
	} else if name == "FIXED_LEN_BYTE_ARRAY" {
		return parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil
	} else if name == "UTF8" {
		return parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
	} else if name == "INT_8" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8)
	} else if name == "INT_16" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16)
	} else if name == "INT_32" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)
	} else if name == "INT_64" {
		return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
	} else if name == "UINT_8" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8)
	} else if name == "UINT_16" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16)
	} else if name == "UINT_32" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32)
	} else if name == "UINT_64" {
		return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64)
	} else if name == "DATE" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DATE)
	} else if name == "TIME_MILLIS" {
		return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS)
	} else if name == "TIME_MICROS" {
		return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS)
	} else if name == "TIMESTAMP_MILLIS" {
		return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS)
	} else if name == "TIMESTAMP_MICROS" {
		return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS)
	} else if name == "INTERVAL" {
		return parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL)
	} else if name == "DECIMAL" {
		if baseName == "INT32" {
			return parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
		} else if baseName == "INT64" {
			return parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
		} else if baseName == "FIXED_LEN_BYTE_ARRAY" {
			return parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
		} else {
			return parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
		}
	}
	return nil, nil
}

func ParquetTypeToGoType(src interface{}, pT *parquet.Type, cT *parquet.ConvertedType) interface{} {
	if src == nil {
		return nil
	}
	if cT == nil {
		if *pT == parquet.Type_BOOLEAN {
			return bool(src.(BOOLEAN))
		} else if *pT == parquet.Type_INT32 {
			return int32(src.(INT32))
		} else if *pT == parquet.Type_INT64 {
			return int64(src.(INT64))
		} else if *pT == parquet.Type_INT96 {
			return string(src.(INT96))
		} else if *pT == parquet.Type_FLOAT {
			return float32(src.(FLOAT))
		} else if *pT == parquet.Type_DOUBLE {
			return float64(src.(DOUBLE))
		} else if *pT == parquet.Type_BYTE_ARRAY {
			return string(src.(BYTE_ARRAY))
		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return string(src.(FIXED_LEN_BYTE_ARRAY))
		}
		return nil
	}

	if *cT == parquet.ConvertedType_UTF8 {
		return string(src.(BYTE_ARRAY))
	} else if *cT == parquet.ConvertedType_INT_8 {
		return int32(src.(INT32))
	} else if *cT == parquet.ConvertedType_INT_16 {
		return int32(src.(INT32))
	} else if *cT == parquet.ConvertedType_INT_32 {
		return int32(src.(INT32))
	} else if *cT == parquet.ConvertedType_INT_64 {
		return int64(src.(INT64))
	} else if *cT == parquet.ConvertedType_UINT_8 {
		return uint32(src.(INT32))
	} else if *cT == parquet.ConvertedType_UINT_16 {
		return uint32(src.(INT32))
	} else if *cT == parquet.ConvertedType_UINT_32 {
		return uint32(src.(INT32))
	} else if *cT == parquet.ConvertedType_UINT_64 {
		return uint64(src.(INT64))
	} else if *cT == parquet.ConvertedType_DATE {
		return int32(src.(INT32))
	} else if *cT == parquet.ConvertedType_TIME_MILLIS {
		return int32(src.(INT32))
	} else if *cT == parquet.ConvertedType_TIME_MICROS {
		return int64(src.(INT64))
	} else if *cT == parquet.ConvertedType_TIMESTAMP_MILLIS {
		return int64(src.(INT64))
	} else if *cT == parquet.ConvertedType_TIMESTAMP_MICROS {
		return int64(src.(INT64))
	} else if *cT == parquet.ConvertedType_INTERVAL {
		return string(src.(FIXED_LEN_BYTE_ARRAY))
	} else if *cT == parquet.ConvertedType_DECIMAL {
		if *pT == parquet.Type_INT32 {
			return int32(src.(INT32))
		} else if *pT == parquet.Type_INT64 {
			return int64(src.(INT64))
		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return string(src.(FIXED_LEN_BYTE_ARRAY))
		} else {
			return string(src.(BYTE_ARRAY))
		}
	} else {
		return nil
	}
}

//Scan a string to parquet value
func StrToParquetType(s string, pT *parquet.Type, cT *parquet.ConvertedType, length int) interface{} {
	if cT == nil {
		if *pT == parquet.Type_BOOLEAN {
			var v BOOLEAN
			fmt.Sscanf(s, "%t", &v)
			return v

		} else if *pT == parquet.Type_INT32 {
			var v INT32
			fmt.Sscanf(s, "%d", &v)
			return v

		} else if *pT == parquet.Type_INT64 {
			var v INT64
			fmt.Sscanf(s, "%d", &v)
			return v

		} else if *pT == parquet.Type_INT96 {
			res := StrIntToBinary(s, "LittleEndian", 12, true)
			return INT96(res)

		} else if *pT == parquet.Type_FLOAT {
			var v FLOAT
			fmt.Sscanf(s, "%f", &v)
			return v

		} else if *pT == parquet.Type_DOUBLE {
			var v DOUBLE
			fmt.Sscanf(s, "%f", &v)
			return v

		} else if *pT == parquet.Type_BYTE_ARRAY {
			return BYTE_ARRAY(s)

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return FIXED_LEN_BYTE_ARRAY(s)
		}
		return nil
	}

	if *cT == parquet.ConvertedType_UTF8 {
		return BYTE_ARRAY(s)

	} else if *cT == parquet.ConvertedType_INT_8 || *cT == parquet.ConvertedType_INT_16 || *cT == parquet.ConvertedType_INT_32 ||
		*cT == parquet.ConvertedType_DATE || *cT == parquet.ConvertedType_TIME_MILLIS {
		var v INT32
		fmt.Sscanf(s, "%d", &v)
		return INT32(v)

	} else if *cT == parquet.ConvertedType_UINT_8 || *cT == parquet.ConvertedType_UINT_16 || *cT == parquet.ConvertedType_UINT_32 {
		var vt uint32
		fmt.Sscanf(s, "%d", &vt)
		return INT32(vt)

	} else if *cT == parquet.ConvertedType_UINT_64 {
		var vt uint64
		fmt.Sscanf(s, "%d", &vt)
		return INT64(vt)

	} else if *cT == parquet.ConvertedType_INT_64 ||
		*cT == parquet.ConvertedType_TIME_MICROS || *cT == parquet.ConvertedType_TIMESTAMP_MICROS || *cT == parquet.ConvertedType_TIMESTAMP_MILLIS {
		var v INT64
		fmt.Sscanf(s, "%d", &v)
		return v

	} else if *cT == parquet.ConvertedType_INTERVAL {
		res := StrIntToBinary(s, "LittleEndian", 12, false)
		return FIXED_LEN_BYTE_ARRAY(res)

	} else if *cT == parquet.ConvertedType_DECIMAL {
		if *pT == parquet.Type_INT32 {
			var v INT32
			fmt.Sscanf(s, "%d", &v)
			return v

		} else if *pT == parquet.Type_INT64 {
			var v INT64
			fmt.Sscanf(s, "%d", &v)
			return v

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			res := StrIntToBinary(s, "BigEndian", length, true)
			return FIXED_LEN_BYTE_ARRAY(res)

		} else {
			res := StrIntToBinary(s, "BigEndian", 0, true)
			return BYTE_ARRAY(res)
		}
	} else {
		return nil
	}
}

func GoTypeToParquetType(src interface{}, pT *parquet.Type, cT *parquet.ConvertedType) interface{} {
	if cT == nil {
		if *pT == parquet.Type_BOOLEAN {
			return BOOLEAN(src.(bool))
		} else if *pT == parquet.Type_INT32 {
			return INT32(src.(int32))
		} else if *pT == parquet.Type_INT64 {
			return INT64(src.(int64))
		} else if *pT == parquet.Type_INT96 {
			return INT96(src.(string))
		} else if *pT == parquet.Type_FLOAT {
			return FLOAT(src.(float32))
		} else if *pT == parquet.Type_DOUBLE {
			return DOUBLE(src.(float64))
		} else if *pT == parquet.Type_BYTE_ARRAY {
			return BYTE_ARRAY(src.(string))
		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return FIXED_LEN_BYTE_ARRAY(src.(string))
		}
		return nil
	}

	if *cT == parquet.ConvertedType_UTF8 {
		return BYTE_ARRAY(src.(string))
	} else if *cT == parquet.ConvertedType_INT_8 {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_INT_16 {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_INT_32 {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_INT_64 {
		return INT64(src.(int64))
	} else if *cT == parquet.ConvertedType_UINT_8 {
		return INT32(src.(uint32))
	} else if *cT == parquet.ConvertedType_UINT_16 {
		return INT32(src.(uint32))
	} else if *cT == parquet.ConvertedType_UINT_32 {
		return INT32(src.(uint32))
	} else if *cT == parquet.ConvertedType_UINT_64 {
		return INT64(src.(uint64))
	} else if *cT == parquet.ConvertedType_DATE {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_TIME_MILLIS {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_TIME_MICROS {
		return INT64(src.(int64))
	} else if *cT == parquet.ConvertedType_TIMESTAMP_MILLIS {
		return INT64(src.(int64))
	} else if *cT == parquet.ConvertedType_TIMESTAMP_MICROS {
		return INT64(src.(int64))
	} else if *cT == parquet.ConvertedType_INTERVAL {
		return FIXED_LEN_BYTE_ARRAY(src.(string))
	} else if *cT == parquet.ConvertedType_DECIMAL {
		if *pT == parquet.Type_INT32 {
			return INT32(src.(int32))
		} else if *pT == parquet.Type_INT64 {
			return INT64(src.(int64))
		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return FIXED_LEN_BYTE_ARRAY(src.(string))
		} else {
			return BYTE_ARRAY(src.(string))
		}
	} else {
		return nil
	}
}

//order=LittleEndian or BigEndian; length is byte num
func StrIntToBinary(num string, order string, length int, signed bool) string {
	bigNum := new(big.Int)
	bigNum.SetString(num, 10)
	if !signed {
		res := bigNum.Bytes()
		if len(res) < length {
			res = append(make([]byte, length-len(res)), res...)
		}
		if order == "LittleEndian" {
			for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
				res[i], res[j] = res[j], res[i]
			}
		}
		if length > 0 {
			res = res[len(res)-length:]
		}
		return string(res)
	}

	flag := bigNum.Cmp(big.NewInt(0))
	if flag == 0 {
		if length <= 0 {
			length = 1
		}
		return string(make([]byte, length))
	}

	bigNum = bigNum.SetBytes(bigNum.Bytes())
	bs := bigNum.Bytes()

	if len(bs) < length {
		bs = append(make([]byte, length-len(bs)), bs...)
	}

	upperBs := make([]byte, len(bs))
	upperBs[0] = byte(0x80)
	upper := new(big.Int)
	upper.SetBytes(upperBs)
	if flag > 0 {
		upper = upper.Sub(upper, big.NewInt(1))
	}

	if bigNum.Cmp(upper) > 0 {
		bs = append(make([]byte, 1), bs...)
	}

	if flag < 0 {
		modBs := make([]byte, len(bs)+1)
		modBs[0] = byte(0x01)
		mod := new(big.Int)
		mod.SetBytes(modBs)
		bs = mod.Sub(mod, bigNum).Bytes()
	}
	if length > 0 {
		bs = bs[len(bs)-length:]
	}
	if order == "LittleEndian" {
		for i, j := 0, len(bs)-1; i < j; i, j = i+1, j-1 {
			bs[i], bs[j] = bs[j], bs[i]
		}
	}

	return string(bs)

}
