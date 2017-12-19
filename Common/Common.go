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
		"keytype":        "",
		"length":         0,
		"keylength":      0,
		"scale":          0,
		"keyscale":       0,
		"precision":      0,
		"keyprecision":   0,
		"fieldid":        0,
		"keyfieldid":     0,
		"repetitiontype": parquet.FieldRepetitionType(0),
		"encoding":       parquet.Encoding_PLAIN,
		"keyencoding":    parquet.Encoding_PLAIN,
	}
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
		if kv[0] == "type" || kv[0] == "keytype" {
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

//Compare two values:
//a>b return 1
//a<b return -1
//a==b return 0
func Cmp(ai interface{}, bi interface{}) int {
	if ai == nil && bi != nil {
		return -1
	} else if ai == nil && bi == nil {
		return 0
	} else if ai != nil && bi == nil {
		return 1
	}

	name := reflect.TypeOf(ai).Name()
	switch name {
	case "BOOLEAN":
		a, b := 0, 0
		if ai.(ParquetType.BOOLEAN) {
			a = 1
		}
		if bi.(ParquetType.BOOLEAN) {
			b = 1
		}
		return a - b

	case "INT32":
		a, b := ai.(ParquetType.INT32), bi.(ParquetType.INT32)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT64":
		a, b := ai.(ParquetType.INT64), bi.(ParquetType.INT64)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT96":
		a, b := []byte(ai.(ParquetType.INT96)), []byte(bi.(ParquetType.INT96))
		fa, fb := a[11]>>7, b[11]>>7
		if fa > fb {
			return -1
		} else if fa < fb {
			return 1
		}
		for i := 11; i >= 0; i-- {
			if a[i] > b[i] {
				return 1
			} else if a[i] < b[i] {
				return -1
			}
		}
		return 0

	case "FLOAT":
		a, b := ai.(ParquetType.FLOAT), bi.(ParquetType.FLOAT)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "DOUBLE":
		a, b := ai.(ParquetType.DOUBLE), bi.(ParquetType.DOUBLE)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "BYTE_ARRAY":
		a, b := ai.(ParquetType.BYTE_ARRAY), bi.(ParquetType.BYTE_ARRAY)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "FIXED_LEN_BYTE_ARRAY":
		a, b := ai.(ParquetType.FIXED_LEN_BYTE_ARRAY), bi.(ParquetType.FIXED_LEN_BYTE_ARRAY)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "UTF8":
		a, b := ai.(ParquetType.UTF8), bi.(ParquetType.UTF8)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT_8":
		a, b := ai.(ParquetType.INT_8), bi.(ParquetType.INT_8)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT_16":
		a, b := ai.(ParquetType.INT_16), bi.(ParquetType.INT_16)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT_32":
		a, b := ai.(ParquetType.INT_32), bi.(ParquetType.INT_32)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT_64":
		a, b := ai.(ParquetType.INT_64), bi.(ParquetType.INT_64)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "UINT_8":
		a, b := ai.(ParquetType.UINT_8), bi.(ParquetType.UINT_8)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "UINT_16":
		a, b := ai.(ParquetType.UINT_16), bi.(ParquetType.UINT_16)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "UINT_32":
		a, b := ai.(ParquetType.UINT_32), bi.(ParquetType.UINT_32)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "UINT_64":
		a, b := ai.(ParquetType.UINT_64), bi.(ParquetType.UINT_64)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "DATE":
		a, b := ai.(ParquetType.DATE), bi.(ParquetType.DATE)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "TIME_MILLIS":
		a, b := ai.(ParquetType.TIME_MILLIS), bi.(ParquetType.TIME_MILLIS)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "TIME_MICROS":
		a, b := ai.(ParquetType.TIME_MICROS), bi.(ParquetType.TIME_MICROS)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "TIMESTAMP_MILLIS":
		a, b := ai.(ParquetType.TIMESTAMP_MILLIS), bi.(ParquetType.TIMESTAMP_MILLIS)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "TIMESTAMP_MICROS":
		a, b := ai.(ParquetType.TIMESTAMP_MICROS), bi.(ParquetType.TIMESTAMP_MICROS)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INTERVAL":
		a, b := []byte(ai.(ParquetType.INTERVAL)), []byte(bi.(ParquetType.INTERVAL))
		for i := 11; i >= 0; i-- {
			if a[i] > b[i] {
				return 1
			} else if a[i] < b[i] {
				return -1
			}
		}
		return 0

	case "DECIMAL":
		a, b := []byte(ai.(ParquetType.DECIMAL)), []byte(bi.(ParquetType.DECIMAL))
		fa, fb := (a[0] >> 7), (b[0] >> 7)
		la, lb := len(a), len(b)
		if fa > fb {
			return -1
		} else if fa < fb {
			return 1
		} else {
			i, j := 0, 0
			for i < la || j < lb {
				ba, bb := byte(0x0), byte(0x0)
				if i < la {
					ba = a[i]
					i++
				}
				if j < lb {
					bb = b[j]
					j++
				}
				if ba > bb {
					return 1
				} else if ba < bb {
					return -1
				}
			}
			return 0
		}
	}
	return 0
}

//Get the maximum of two parquet values
func Max(a interface{}, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if Cmp(a, b) > 0 {
		return a
	}
	return b
}

//Get the minimum of two parquet values
func Min(a interface{}, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if Cmp(a, b) > 0 {
		return b
	}
	return a
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
