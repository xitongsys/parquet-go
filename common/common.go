package common

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/xitongsys/parquet-go/parquet"
)

// `parquet:"name=Name, type=FIXED_LEN_BYTE_ARRAY, length=12"`
type Tag struct {
	InName string
	ExName string

	Type      string
	KeyType   string
	ValueType string

	BaseType      string
	KeyBaseType   string
	ValueBaseType string

	Length      int32
	KeyLength   int32
	ValueLength int32

	Scale      int32
	KeyScale   int32
	ValueScale int32

	Precision      int32
	KeyPrecision   int32
	ValuePrecision int32

	FieldID      int32
	KeyFieldID   int32
	ValueFieldID int32

	Encoding      parquet.Encoding
	KeyEncoding   parquet.Encoding
	ValueEncoding parquet.Encoding

	RepetitionType      parquet.FieldRepetitionType
	KeyRepetitionType   parquet.FieldRepetitionType
	ValueRepetitionType parquet.FieldRepetitionType
}

func NewTag() *Tag {
	return &Tag{}
}

func StringToTag(tag string) *Tag {
	mp := NewTag()
	tagStr := strings.Replace(tag, "\t", "", -1)
	tags := strings.Split(tagStr, ",")

	for _, tag := range tags {
		tag = strings.TrimSpace(tag)

		kv := strings.Split(tag, "=")

		key := kv[0]
		key = strings.ToLower(key)
		key = strings.TrimSpace(key)

		val := kv[1]
		val = strings.TrimSpace(val)

		valInt32 := func() int32 {
			valInt, err := strconv.Atoi(val)
			if err != nil {
				panic(err)
			}
			return int32(valInt)
		}

		switch key {
		case "type":
			mp.Type = val
		case "keytype":
			mp.KeyType = val
		case "valuetype":
			mp.ValueType = val
		case "basetype":
			mp.BaseType = val
		case "keybasetype":
			mp.KeyBaseType = val
		case "valuebasetype":
			mp.ValueBaseType = val
		case "length":
			mp.Length = valInt32()
		case "keylength":
			mp.KeyLength = valInt32()
		case "valuelength":
			mp.ValueLength = valInt32()
		case "scale":
			mp.Scale = valInt32()
		case "keyscale":
			mp.KeyScale = valInt32()
		case "valuescale":
			mp.ValueScale = valInt32()
		case "precision":
			mp.Precision = valInt32()
		case "keyprecision":
			mp.KeyPrecision = valInt32()
		case "valueprecision":
			mp.ValuePrecision = valInt32()
		case "fieldid":
			mp.FieldID = valInt32()
		case "keyfieldid":
			mp.KeyFieldID = valInt32()
		case "valuefieldid":
			mp.ValueFieldID = valInt32()
		case "name":
			if mp.InName == "" {
				mp.InName = StringToVariableName(val)
			}
			mp.ExName = val
		case "inname":
			mp.InName = val
		case "repetitiontype":
			switch strings.ToLower(val) {
			case "repeated":
				mp.RepetitionType = parquet.FieldRepetitionType_REPEATED
			case "required":
				mp.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			case "optional":
				mp.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
			default:
				panic(fmt.Errorf("Unknown repetitiontype: '%v'", val))
			}
		case "keyrepetitiontype":
			switch strings.ToLower(val) {
			case "repeated":
				mp.KeyRepetitionType = parquet.FieldRepetitionType_REPEATED
			case "required":
				mp.KeyRepetitionType = parquet.FieldRepetitionType_REQUIRED
			case "optional":
				mp.KeyRepetitionType = parquet.FieldRepetitionType_OPTIONAL
			default:
				panic(fmt.Errorf("Unknown keyrepetitiontype: '%v'", val))
			}
		case "valuerepetitiontype":
			switch strings.ToLower(val) {
			case "repeated":
				mp.ValueRepetitionType = parquet.FieldRepetitionType_REPEATED
			case "required":
				mp.ValueRepetitionType = parquet.FieldRepetitionType_REQUIRED
			case "optional":
				mp.ValueRepetitionType = parquet.FieldRepetitionType_OPTIONAL
			default:
				panic(fmt.Errorf("Unknown valuerepetitiontype: '%v'", val))
			}
		case "encoding":
			switch strings.ToLower(val) {
			case "plain":
				mp.Encoding = parquet.Encoding_PLAIN
			case "rle":
				mp.Encoding = parquet.Encoding_RLE
			case "delta_binary_packed":
				mp.Encoding = parquet.Encoding_DELTA_BINARY_PACKED
			case "delta_length_byte_array":
				mp.Encoding = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
			case "delta_byte_array":
				mp.Encoding = parquet.Encoding_DELTA_BYTE_ARRAY
			case "plain_dictionary":
				mp.Encoding = parquet.Encoding_PLAIN_DICTIONARY
			case "rle_dictionary":
				mp.Encoding = parquet.Encoding_RLE_DICTIONARY
			default:
				panic(fmt.Errorf("Unknown encoding type: '%v'", val))
			}
		case "keyencoding":
			switch strings.ToLower(val) {
			case "rle":
				mp.KeyEncoding = parquet.Encoding_RLE
			case "delta_binary_packed":
				mp.KeyEncoding = parquet.Encoding_DELTA_BINARY_PACKED
			case "delta_length_byte_array":
				mp.KeyEncoding = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
			case "delta_byte_array":
				mp.KeyEncoding = parquet.Encoding_DELTA_BYTE_ARRAY
			case "plain_dictionary":
				mp.KeyEncoding = parquet.Encoding_PLAIN_DICTIONARY
			default:
				panic(fmt.Errorf("Unknown keyencoding type: '%v'", val))
			}
		case "valueencoding":
			switch strings.ToLower(val) {
			case "rle":
				mp.ValueEncoding = parquet.Encoding_RLE
			case "delta_binary_packed":
				mp.ValueEncoding = parquet.Encoding_DELTA_BINARY_PACKED
			case "delta_length_byte_array":
				mp.ValueEncoding = parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
			case "delta_byte_array":
				mp.ValueEncoding = parquet.Encoding_DELTA_BYTE_ARRAY
			case "plain_dictionary":
				mp.ValueEncoding = parquet.Encoding_PLAIN_DICTIONARY
			default:
				panic(fmt.Errorf("Unknown valueencoding type: '%v'", val))
			}
		default:
			panic(fmt.Errorf("Unrecognized tag '%v'", key))
		}
	}
	return mp
}

func NewSchemaElementFromTagMap(info *Tag) *parquet.SchemaElement {
	schema := parquet.NewSchemaElement()
	schema.Name = info.InName
	schema.TypeLength = &info.Length
	schema.Scale = &info.Scale
	schema.Precision = &info.Precision
	schema.FieldID = &info.FieldID
	schema.RepetitionType = &info.RepetitionType
	schema.NumChildren = nil

	typeName := info.Type
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
		} else if typeName == "UTF8" || typeName == "JSON" || typeName == "BSON" {
			schema.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		} else if typeName == "INTERVAL" {
			schema.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
			var ln int32 = 12
			schema.TypeLength = &ln
		} else if typeName == "DECIMAL" {
			t, _ = parquet.TypeFromString(info.BaseType)
			schema.Type = &t
		}
	}
	return schema
}

func DeepCopy(src, dst interface{}) {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(src)
	gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
	return
}

//Get key tag map for map
func GetKeyTagMap(src *Tag) *Tag {
	res := NewTag()
	res.InName = "Key"
	res.ExName = "key"
	res.Type = src.KeyType
	res.BaseType = src.KeyBaseType
	res.Length = src.KeyLength
	res.Scale = src.KeyScale
	res.Precision = src.KeyPrecision
	res.FieldID = src.KeyFieldID
	res.Encoding = src.KeyEncoding
	res.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	return res
}

//Get value tag map for map
func GetValueTagMap(src *Tag) *Tag {
	res := NewTag()
	res.InName = "Value"
	res.ExName = "value"
	res.Type = src.ValueType
	res.BaseType = src.ValueBaseType
	res.Length = src.ValueLength
	res.Scale = src.ValueScale
	res.Precision = src.ValuePrecision
	res.FieldID = src.ValueFieldID
	res.Encoding = src.ValueEncoding
	res.RepetitionType = src.ValueRepetitionType
	return res
}

//Convert string to a golang variable name
func StringToVariableName(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	name := ""
	for i := 0; i<ln; i++ {
		c := str[i]
		if (c>='a' && c<='z') || (c>='A' && c<='Z') || (c>='0' && c<='9') || c == '_' {
			name += string(c)

		} else {
			name += strconv.Itoa(int(c))
		}
	}

	name = HeadToUpper(name)
	return name
}

//Convert the first letter of a string to uppercase
func HeadToUpper(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	c := str[0]
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return strings.ToUpper(str[0:1]) + str[1:]
	}
	//handle non-alpha prefix such as "_"
	return "P_" + str
}

//Get the number of bits needed by the num; 0 needs 0, 1 need 1, 2 need 2, 3 need 2 ....
func BitNum(num uint64) uint64 {
	var bitn uint64 = 0
	for ; num != 0; num >>= 1 {
		bitn++
	}
	return bitn
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
			a, b := ai.(bool), bi.(bool)
			if !a && b {
				return true
			}
			return false
		} else if *pT == parquet.Type_INT32 {
			return ai.(int32) < bi.(int32)

		} else if *pT == parquet.Type_INT64 {
			return ai.(int64) < bi.(int64)

		} else if *pT == parquet.Type_INT96 {
			a, b := []byte(ai.(string)), []byte(bi.(string))
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
			return ai.(float32) < bi.(float32)

		} else if *pT == parquet.Type_DOUBLE {
			return ai.(float64) < bi.(float64)

		} else if *pT == parquet.Type_BYTE_ARRAY {
			return ai.(string) < bi.(string)

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return ai.(string) < bi.(string)
		}
	}

	if *cT == parquet.ConvertedType_UTF8 || *cT == parquet.ConvertedType_BSON || *cT == parquet.ConvertedType_JSON {
		return ai.(string) < bi.(string)

	} else if *cT == parquet.ConvertedType_INT_8 || *cT == parquet.ConvertedType_INT_16 || *cT == parquet.ConvertedType_INT_32 ||
		*cT == parquet.ConvertedType_DATE || *cT == parquet.ConvertedType_TIME_MILLIS {
		return ai.(int32) < bi.(int32)

	} else if *cT == parquet.ConvertedType_UINT_8 || *cT == parquet.ConvertedType_UINT_16 || *cT == parquet.ConvertedType_UINT_32 {
		return uint32(ai.(int32)) < uint32(bi.(int32))

	} else if *cT == parquet.ConvertedType_INT_64 || *cT == parquet.ConvertedType_TIME_MICROS ||
		*cT == parquet.ConvertedType_TIMESTAMP_MILLIS || *cT == parquet.ConvertedType_TIMESTAMP_MICROS {
		return ai.(int64) < bi.(int64)

	} else if *cT == parquet.ConvertedType_UINT_64 {
		return uint64(ai.(int64)) < uint64(bi.(int64))

	} else if *cT == parquet.ConvertedType_INTERVAL {
		a, b := []byte(ai.(string)), []byte(bi.(string))
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
			as, bs := ai.(string), bi.(string)
			return CmpIntBinary(as, bs, "BigEndian", true)

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			as, bs := ai.(string), bi.(string)
			return CmpIntBinary(as, bs, "BigEndian", true)

		} else if *pT == parquet.Type_INT32 {
			return ai.(int32) < bi.(int32)

		} else if *pT == parquet.Type_INT64 {
			return ai.(int64) < bi.(int64)

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
	var size int64
	switch val.Type().Kind() {
	case reflect.Ptr:
		if val.IsNil() {
			return 0
		}
		return SizeOf(val.Elem())
	case reflect.Slice:
		for i := 0; i < val.Len(); i++ {
			size += SizeOf(val.Index(i))
		}
		return size
	case reflect.Struct:
		for i := 0; i < val.Type().NumField(); i++ {
			size += SizeOf(val.Field(i))
		}
		return size
	case reflect.Map:
		keys := val.MapKeys()
		for i := 0; i < len(keys); i++ {
			size += SizeOf(keys[i])
			size += SizeOf(val.MapIndex(keys[i]))
		}
		return size
	}
	switch val.Type().Name() {
	case "bool":
		return 1
	case "int32":
		return 4
	case "int64":
		return 8
	case "string":
		return int64(val.Len())
	case "float32":
		return 4
	case "float64":
		return 8
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

//Get the pathStr index in a path
func PathStrIndex(str string) int {
	return len(strings.Split(str, "."))
}
