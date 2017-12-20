package Common

import (
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
	"strconv"
	"strings"
)

//Parse the tag to map; tag format is:
//`parquet:"name=Name, type=FIXED_LEN_BYTE_ARRAY, length=12"`
func NewEmptyTagMap() map[string]interface{} {
	return map[string]interface{}{
		"inname":         "",
		"exname":         "",
		"type":           "",
		"basetype":       "", //only for decimal
		"keytype":        "",
		"basekeytype":    "", //only for decimal
		"length":         int32(0),
		"keylength":      int32(0),
		"scale":          int32(0),
		"keyscale":       int32(0),
		"precision":      int32(0),
		"keyprecision":   int32(0),
		"fieldid":        int32(0),
		"keyfieldid":     int32(0),
		"repetitiontype": parquet.FieldRepetitionType(0),
		"encoding":       parquet.Encoding_PLAIN,
		"keyencoding":    parquet.Encoding_PLAIN,
		"bitwidth":       int32(0),
	}
}

func NewSchemaElementFromTagMap(info map[string]interface{}) *parquet.SchemaElement {
	schema := parquet.NewSchemaElement()

	inname := info["inname"].(string)
	length := info["length"].(int32)
	scale := info["scale"].(int32)
	precision := info["precision"].(int32)
	fieldid := info["fieldid"].(int32)
	repetitiontype := info["repetitiontype"].(parquet.FieldRepetitionType)

	schema.Name = inname
	schema.TypeLength = &length
	schema.Scale = &scale
	schema.Precision = &precision
	schema.FieldID = &fieldid
	schema.RepetitionType = &repetitiontype
	schema.NumChildren = nil

	typeName := info["type"].(string)
	if t, err := parquet.TypeFromString(typeName); err == nil {
		schema.Type = &t
	} else {
		ct, _ := parquet.ConvertedTypeFromString(typeName)
		schema.ConvertedType = &ct
		if typeName == "INT_8" || typeName == "INT_16" || typeName == "INT_32" ||
			typeName == "UINT_8" || typeName == "UINT_16" || typeName == "UINT_32" ||
			typeName == "DATE" || typeName == "TIME_MILLIS" {
			schema.Type = parquet.TypePtr(parquet.Type_INT32)
		} else if typeName == "INT_64" || typeName == "UINT_64" ||
			typeName == "TIME_MICROS" || typeName == "TIMESTAMP_MICROS" || typeName == "TIMESTAMP_MILLIS" {
			schema.Type = parquet.TypePtr(parquet.Type_INT64)
		} else if typeName == "UTF8" {
			schema.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		} else if typeName == "INTERVAL" {
			schema.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
			var ln int32 = 12
			schema.TypeLength = &ln
		} else if typeName == "DECIMAL" {
			t, _ = parquet.TypeFromString(info["basetype"].(string))
			schema.Type = &t
		}
	}
	return schema
}

func NewTagMapFromCopy(tagMap map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for key, val := range tagMap {
		res[key] = val
	}
	return res
}

func TagToMap(tag string) map[string]interface{} {
	mp := NewEmptyTagMap()
	tagStr := strings.Replace(tag, " ", "", -1)
	tagStr = strings.Replace(tagStr, "\t", "", -1)
	tags := strings.Split(tagStr, ",")
	for _, tag := range tags {
		kv := strings.Split(tag, "=")
		kv[0] = strings.ToLower(kv[0])
		if kv[0] == "type" || kv[0] == "keytype" || kv[0] == "basetype" || kv[0] == "basekeytype" {
			mp[kv[0]] = kv[1]
		} else if kv[0] == "length" || kv[0] == "keylength" ||
			kv[0] == "scale" || kv[0] == "keyscale" ||
			kv[0] == "precision" || kv[0] == "keyprecision" ||
			kv[0] == "fieldid" || kv[0] == "keyfieldid" {

			val, _ := strconv.Atoi(kv[1])
			mp[kv[0]] = int32(val)
		} else if kv[0] == "name" {
			mp["inname"] = kv[1]
			mp["exname"] = kv[1]
		} else if kv[0] == "repetitiontype" {
			switch kv[1] {
			case "repeated":
				mp["repetitiontype"] = parquet.FieldRepetitionType_REPEATED
			case "required":
				mp["repetitiontype"] = parquet.FieldRepetitionType_REQUIRED
			case "optional":
				mp["repetitiontype"] = parquet.FieldRepetitionType_OPTIONAL
			}
		} else if kv[0] == "encoding" || kv[0] == "keyencoding" {
			ens := strings.ToLower(kv[1])
			if ens == "rle" {
				mp[kv[0]] = parquet.Encoding_RLE
			} else if ens == "delta_binary_packed" {
				mp[kv[0]] = parquet.Encoding_DELTA_BINARY_PACKED
			} else if ens == "delta_length_byte_array" {
				mp[kv[0]] = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
			} else if ens == "delta_byte_array" {
				mp[kv[0]] = parquet.Encoding_DELTA_BYTE_ARRAY
			} else {
				mp[kv[0]] = parquet.Encoding_PLAIN
			}
		}
	}
	return mp
}

//Get key tag map for map
func GetKeyTagMap(src map[string]interface{}) map[string]interface{} {
	res := NewEmptyTagMap()
	res["inname"] = "key"
	res["exname"] = "key"
	res["type"] = src["keytype"]
	res["basetype"] = src["basekeytype"]
	res["length"] = src["keylength"]
	res["scale"] = src["keyscale"]
	res["precision"] = src["keyprecision"]
	res["fieldid"] = src["keyfieldid"]
	res["encoding"] = src["keyencoding"]
	return res
}

//Get value tag map for map
func GetValueTagMap(src map[string]interface{}) map[string]interface{} {
	res := NewEmptyTagMap()
	res["inname"] = "value"
	res["exname"] = "value"
	res["type"] = src["type"]
	res["basetype"] = src["basetype"]
	res["length"] = src["length"]
	res["scale"] = src["scale"]
	res["precision"] = src["precision"]
	res["fieldid"] = src["fieldid"]
	res["repetitiontype"] = src["repetitiontype"]
	res["encoding"] = src["encoding"]
	return res
}

//Convert the first letter of a string to uppercase
func HeadToUpper(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}
	return strings.ToUpper(str[0:1]) + str[1:]
}

//Get the number of bits needed by the num; 0 needs 0, 1 need 1, 2 need 2, 3 need 2 ....
func BitNum(num uint64) uint64 {
	var bitn int64 = 63
	for (bitn >= 0) && (((1 << uint64(bitn)) & num) == 0) {
		bitn--
	}
	return uint64(bitn + 1)
}

func CmpIntBinary(as string, bs string, order string, signed bool) bool {
	abs, bbs := []byte(as), []byte(bs)
	la, lb := len(abs), len(bbs)

	if order == "LittleEndian" {
		for i, j := 0, len(abs)-1; i < j; i, j = i+1, j-1 {
			abs[i], abs[j] = abs[j], abs[i]
		}
		for i, j := 0, len(bbs)-1; i < j; i, j = i+1, j-1 {
			bbs[i], bbs[j] = bbs[j], bbs[i]
		}
	}
	if !signed {
		if la < lb {
			abs = append(make([]byte, lb-la), abs...)
		} else if lb < la {
			bbs = append(make([]byte, la-lb), bbs...)
		}
	} else {
		if la < lb {
			sb := (abs[0] >> 7) & 1
			pre := make([]byte, lb-la)
			if sb == 1 {
				for i := 0; i < lb-la; i++ {
					pre[i] = byte(0xFF)
				}
			}
			abs = append(pre, abs...)

		} else if la > lb {
			sb := (bbs[0] >> 7) & 1
			pre := make([]byte, la-lb)
			if sb == 1 {
				for i := 0; i < la-lb; i++ {
					pre[i] = byte(0xFF)
				}
			}
			bbs = append(pre, bbs...)
		}

		asb, bsb := (abs[0]>>7)&1, (bbs[0]>>7)&1

		if asb < bsb {
			return false
		} else if asb > bsb {
			return true
		}

	}

	for i := 0; i < len(abs); i++ {
		if abs[i] < bbs[i] {
			return true
		} else if abs[i] > bbs[i] {
			return false
		}
	}
	return false
}

//Compare two values:
//a<b return true
//a>=b return false
func Cmp(ai interface{}, bi interface{}, pT *parquet.Type, cT *parquet.ConvertedType) bool {
	if ai == nil && bi != nil {
		return true
	} else if ai == nil && bi == nil {
		return false
	} else if ai != nil && bi == nil {
		return false
	}

	if cT == nil {
		if *pT == parquet.Type_BOOLEAN {
			a, b := ai.(ParquetType.BOOLEAN), bi.(ParquetType.BOOLEAN)
			if !a && b {
				return true
			}
			return false
		} else if *pT == parquet.Type_INT32 {
			return ai.(ParquetType.INT32) < bi.(ParquetType.INT32)

		} else if *pT == parquet.Type_INT64 {
			return ai.(ParquetType.INT64) < bi.(ParquetType.INT64)

		} else if *pT == parquet.Type_INT96 {
			a, b := []byte(ai.(ParquetType.INT96)), []byte(bi.(ParquetType.INT96))
			fa, fb := a[11]>>7, b[11]>>7
			if fa > fb {
				return true
			} else if fa < fb {
				return false
			}
			for i := 11; i >= 0; i-- {
				if a[i] < b[i] {
					return true
				} else if a[i] > b[i] {
					return false
				}
			}
			return false

		} else if *pT == parquet.Type_FLOAT {
			return ai.(ParquetType.FLOAT) < bi.(ParquetType.FLOAT)

		} else if *pT == parquet.Type_DOUBLE {
			return ai.(ParquetType.DOUBLE) < bi.(ParquetType.DOUBLE)

		} else if *pT == parquet.Type_BYTE_ARRAY {
			return ai.(ParquetType.BYTE_ARRAY) < bi.(ParquetType.BYTE_ARRAY)

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return ai.(ParquetType.FIXED_LEN_BYTE_ARRAY) < bi.(ParquetType.FIXED_LEN_BYTE_ARRAY)
		}
	}

	if *cT == parquet.ConvertedType_UTF8 {
		return ai.(ParquetType.BYTE_ARRAY) < bi.(ParquetType.BYTE_ARRAY)

	} else if *cT == parquet.ConvertedType_INT_8 || *cT == parquet.ConvertedType_INT_16 || *cT == parquet.ConvertedType_INT_32 ||
		*cT == parquet.ConvertedType_DATE || *cT == parquet.ConvertedType_TIME_MILLIS {
		return ai.(ParquetType.INT32) < bi.(ParquetType.INT32)

	} else if *cT == parquet.ConvertedType_UINT_8 || *cT == parquet.ConvertedType_UINT_16 || *cT == parquet.ConvertedType_UINT_32 {
		return uint32(ai.(ParquetType.INT32)) < uint32(bi.(ParquetType.INT32))

	} else if *cT == parquet.ConvertedType_INT_64 || *cT == parquet.ConvertedType_TIME_MICROS ||
		*cT == parquet.ConvertedType_TIMESTAMP_MILLIS || *cT == parquet.ConvertedType_TIMESTAMP_MICROS {
		return ai.(ParquetType.INT64) < bi.(ParquetType.INT64)

	} else if *cT == parquet.ConvertedType_UINT_64 {
		return uint64(ai.(ParquetType.INT64)) < uint64(bi.(ParquetType.INT64))

	} else if *cT == parquet.ConvertedType_INTERVAL {
		a, b := []byte(ai.(ParquetType.FIXED_LEN_BYTE_ARRAY)), []byte(bi.(ParquetType.FIXED_LEN_BYTE_ARRAY))
		for i := 11; i >= 0; i-- {
			if a[i] > b[i] {
				return false
			} else if a[i] < b[i] {
				return true
			}
		}
		return false

	} else if *cT == parquet.ConvertedType_DECIMAL {
		if *pT == parquet.Type_BYTE_ARRAY {
			as, bs := string(ai.(ParquetType.BYTE_ARRAY)), string(bi.(ParquetType.BYTE_ARRAY))
			return CmpIntBinary(as, bs, "BigEndian", true)

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			as, bs := string(ai.(ParquetType.FIXED_LEN_BYTE_ARRAY)), string(bi.(ParquetType.FIXED_LEN_BYTE_ARRAY))
			return CmpIntBinary(as, bs, "BigEndian", true)

		} else if *pT == parquet.Type_INT32 {
			return ai.(ParquetType.INT32) < bi.(ParquetType.INT32)

		} else if *pT == parquet.Type_INT64 {
			return ai.(ParquetType.INT64) < bi.(ParquetType.INT64)

		}
	}
	return false
}

//Get the maximum of two parquet values
func Max(a interface{}, b interface{}, pT *parquet.Type, cT *parquet.ConvertedType) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if Cmp(a, b, pT, cT) {
		return b
	}
	return a
}

//Get the minimum of two parquet values
func Min(a interface{}, b interface{}, pT *parquet.Type, cT *parquet.ConvertedType) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if Cmp(a, b, pT, cT) {
		return a
	}
	return b
}

//Get the size of a parquet value
func SizeOf(val reflect.Value) int64 {
	tk := val.Type().Kind()

	if tk == reflect.Ptr {
		if val.IsNil() {
			return 0
		}
		val = val.Elem()
		return SizeOf(val)
	}

	if tk == reflect.Slice {
		var size int64 = 0
		for i := 0; i < val.Len(); i++ {
			size += SizeOf(val.Index(i))
		}
		return size
	} else if tk == reflect.Struct {
		var size int64 = 0
		for i := 0; i < val.Type().NumField(); i++ {
			size += SizeOf(val.Field(i))
		}
		return size

	} else if tk == reflect.Map {
		var size int64 = 0
		keys := val.MapKeys()
		for i := 0; i < len(keys); i++ {
			size += SizeOf(keys[i])
			size += SizeOf(val.MapIndex(keys[i]))
		}
		return size
	}

	switch val.Type().Name() {
	case "BOOLEAN":
		return 1
	case "INT32":
		return 4
	case "INT64":
		return 8
	case "INT96":
		return 12
	case "FLOAT":
		return 4
	case "DOUBLE":
		return 8
	case "BYTE_ARRAY":
		return int64(val.Len())
	case "FIXED_LEN_BYTE_ARRAY":
		return int64(val.Len())
	case "UTF8":
		return int64(val.Len())
	case "INT_8":
		return 4
	case "INT_16":
		return 4
	case "INT_32":
		return 4
	case "INT_64":
		return 8
	case "UINT_8":
		return 4
	case "UINT_16":
		return 4
	case "UINT_32":
		return 4
	case "UINT_64":
		return 8
	case "DATE":
		return 4
	case "TIME_MILLIS":
		return 4
	case "TIME_MICROS":
		return 8
	case "TIMESTAMP_MILLIS":
		return 8
	case "TIMESTAMP_MICROS":
		return 8
	case "INTERVAL":
		return 12
	case "DECIMAL":
		return int64(val.Len())
	}

	return 4
}

//Convert path slice to string
func PathToStr(path []string) string {
	return strings.Join(path, ".")
}

//Convert string to path slice
func StrToPath(str string) []string {
	return strings.Split(str, ".")
}
