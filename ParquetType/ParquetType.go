package ParquetType

import (
	"fmt"
	"math/big"
	"reflect"

	"github.com/xitongsys/parquet-go/parquet"
)

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
		return src
	}

	if *cT == parquet.ConvertedType_UINT_8 {
		return uint32(src.(int32))
	} else if *cT == parquet.ConvertedType_UINT_16 {
		return uint32(src.(int32))
	} else if *cT == parquet.ConvertedType_UINT_32 {
		return uint32(src.(int32))
	} else if *cT == parquet.ConvertedType_UINT_64 {
		return uint64(src.(int64))
	} else {
		return src
	}
}

func GoTypeToParquetType(src interface{}, pT *parquet.Type, cT *parquet.ConvertedType) interface{} {
	if cT == nil {
		return src
	}

	if *cT == parquet.ConvertedType_UINT_8 {
		return int32(src.(uint32))
	} else if *cT == parquet.ConvertedType_UINT_16 {
		return int32(src.(uint32))
	} else if *cT == parquet.ConvertedType_UINT_32 {
		return int32(src.(uint32))
	} else if *cT == parquet.ConvertedType_UINT_64 {
		return int64(src.(uint64))
	} else {
		return src
	}
}

//Scan a string to parquet value; length and scale just for decimal
func StrToParquetType(s string, pT *parquet.Type, cT *parquet.ConvertedType, length int, scale int) interface{} {
	if cT == nil {
		if *pT == parquet.Type_BOOLEAN {
			var v bool
			fmt.Sscanf(s, "%t", &v)
			return v

		} else if *pT == parquet.Type_INT32 {
			var v int32
			fmt.Sscanf(s, "%d", &v)
			return v

		} else if *pT == parquet.Type_INT64 {
			var v int64
			fmt.Sscanf(s, "%d", &v)
			return v

		} else if *pT == parquet.Type_INT96 {
			res := StrIntToBinary(s, "LittleEndian", 12, true)
			return res

		} else if *pT == parquet.Type_FLOAT {
			var v float32
			fmt.Sscanf(s, "%f", &v)
			return v

		} else if *pT == parquet.Type_DOUBLE {
			var v float64
			fmt.Sscanf(s, "%f", &v)
			return v

		} else if *pT == parquet.Type_BYTE_ARRAY {
			return s

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return s
		}
		return nil
	}

	if *cT == parquet.ConvertedType_UTF8 {
		return s

	} else if *cT == parquet.ConvertedType_INT_8 || *cT == parquet.ConvertedType_INT_16 || *cT == parquet.ConvertedType_INT_32 ||
		*cT == parquet.ConvertedType_DATE || *cT == parquet.ConvertedType_TIME_MILLIS {
		var v int32
		fmt.Sscanf(s, "%d", &v)
		return v

	} else if *cT == parquet.ConvertedType_UINT_8 || *cT == parquet.ConvertedType_UINT_16 || *cT == parquet.ConvertedType_UINT_32 {
		var vt uint32
		fmt.Sscanf(s, "%d", &vt)
		return int32(vt)

	} else if *cT == parquet.ConvertedType_UINT_64 {
		var vt uint64
		fmt.Sscanf(s, "%d", &vt)
		return int64(vt)

	} else if *cT == parquet.ConvertedType_INT_64 ||
		*cT == parquet.ConvertedType_TIME_MICROS || *cT == parquet.ConvertedType_TIMESTAMP_MICROS || *cT == parquet.ConvertedType_TIMESTAMP_MILLIS {
		var v int64
		fmt.Sscanf(s, "%d", &v)
		return v

	} else if *cT == parquet.ConvertedType_INTERVAL {
		res := StrIntToBinary(s, "LittleEndian", 12, false)
		return res

	} else if *cT == parquet.ConvertedType_DECIMAL {
		numSca := big.NewFloat(1.0)
		for i := 0; i < scale; i++ {
			numSca.Mul(numSca, big.NewFloat(10))
		}
		num := new(big.Float)
		num.SetString(s)
		num.Mul(num, numSca)

		if *pT == parquet.Type_INT32 {
			tmp, _ := num.Float64()
			return int32(tmp)

		} else if *pT == parquet.Type_INT64 {
			tmp, _ := num.Float64()
			return int64(tmp)

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			s = num.String()
			res := StrIntToBinary(s, "BigEndian", length, true)
			return res

		} else {
			s = num.String()
			res := StrIntToBinary(s, "BigEndian", 0, true)
			return res
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

func JSONTypeToParquetType(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, length int, scale int) interface{} {
	if val.Type().Kind() == reflect.Interface && val.IsNil() {
		return nil
	}
	s := fmt.Sprintf("%v", val)
	return StrToParquetType(s, pT, cT, length, scale)
}
