package common

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/hangxie/parquet-go/v2/parquet"
)

type fieldAttr struct {
	Type           string
	Length         int32
	Scale          int32
	Precision      int32
	Encoding       parquet.Encoding
	OmitStats      bool
	RepetitionType parquet.FieldRepetitionType

	convertedType     string
	isAdjustedToUTC   bool
	fieldID           int32
	logicalTypeFields map[string]string
}

func (mp *fieldAttr) update(key, val string) error {
	var err error
	switch key {
	case "type":
		mp.Type = val
	case "convertedtype":
		mp.convertedType = val
	case "length":
		if mp.Length, err = str2Int32(val); err != nil {
			return fmt.Errorf("failed to parse length: %s", err.Error())
		}
	case "scale":
		if mp.Scale, err = str2Int32(val); err != nil {
			return fmt.Errorf("failed to parse scale: %s", err.Error())
		}
	case "precision":
		if mp.Precision, err = str2Int32(val); err != nil {
			return fmt.Errorf("failed to parse precision: %s", err.Error())
		}
	case "fieldid":
		if mp.fieldID, err = str2Int32(val); err != nil {
			return fmt.Errorf("failed to parse fieldid: %s", err.Error())
		}
	case "isadjustedtoutc":
		if mp.isAdjustedToUTC, err = str2Bool(val); err != nil {
			return fmt.Errorf("failed to parse isadjustedtoutc: %s", err.Error())
		}
	case "omitstats":
		if mp.OmitStats, err = str2Bool(val); err != nil {
			return fmt.Errorf("failed to parse omitstats: %s", err.Error())
		}
	case "repetitiontype":
		mp.RepetitionType, err = parquet.FieldRepetitionTypeFromString(strings.ToUpper(val))
		if err != nil {
			return fmt.Errorf("failed to parse repetitiontype: %w", err)
		}
	case "encoding":
		mp.Encoding, err = parquet.EncodingFromString(strings.ToUpper(val))
		if err != nil {
			return fmt.Errorf("failed to parse encoding: %w", err)
		}
	default:
		if strings.HasPrefix(key, "logicaltype") {
			if mp.logicalTypeFields == nil {
				mp.logicalTypeFields = make(map[string]string)
			}
			mp.logicalTypeFields[key] = val
		} else {
			return fmt.Errorf("unrecognized tag '%v'", key)
		}
	}
	return nil
}

type Tag struct {
	InName string
	ExName string
	fieldAttr
	Key   fieldAttr
	Value fieldAttr
}

func NewTag() *Tag {
	return &Tag{}
}

func StringToTag(tag string) (*Tag, error) {
	mp := NewTag()
	tagStr := strings.Replace(tag, "\t", "", -1)

	for tag := range strings.SplitSeq(tagStr, ",") {
		kv := strings.SplitN(tag, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("expect 'key=value' but got '%s'", tag)
		}
		key, val := kv[0], kv[1]
		key = strings.ToLower(key)
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		if key == "name" {
			if mp.InName == "" {
				mp.InName = StringToVariableName(val)
			}
			mp.ExName = val
			continue
		}

		if key == "inname" {
			mp.InName = val
			continue
		}

		var err error
		if strings.HasPrefix(key, "key") {
			err = mp.Key.update(strings.TrimPrefix(key, "key"), val)
		} else if strings.HasPrefix(key, "value") {
			err = mp.Value.update(strings.TrimPrefix(key, "value"), val)
		} else {
			err = mp.fieldAttr.update(key, val)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse tag '%s': %w", tag, err)
		}
	}
	return mp, nil
}

func NewSchemaElementFromTagMap(info *Tag) (*parquet.SchemaElement, error) {
	schema := parquet.NewSchemaElement()
	schema.Name = info.InName
	schema.TypeLength = &info.Length
	schema.Scale = &info.Scale
	schema.Precision = &info.Precision
	schema.FieldID = &info.fieldID
	schema.RepetitionType = &info.RepetitionType
	schema.NumChildren = nil

	if t, err := parquet.TypeFromString(info.Type); err == nil {
		schema.Type = &t
	} else {
		return nil, fmt.Errorf("field [%s] with type [%s]: %s", info.InName, info.Type, err.Error())
	}

	if ct, err := parquet.ConvertedTypeFromString(info.convertedType); err == nil {
		schema.ConvertedType = &ct
	}

	var logicalType *parquet.LogicalType
	var err error
	if len(info.logicalTypeFields) > 0 {
		logicalType, err = newLogicalTypeFromFieldsMap(info.logicalTypeFields)
		if err != nil {
			return nil, fmt.Errorf("failed to create logicaltype from field map: %s", err.Error())
		}
	} else {
		logicalType = newLogicalTypeFromConvertedType(schema, info)
	}

	schema.LogicalType = logicalType

	return schema, nil
}

func newTimeUnitFromString(unitStr string) (*parquet.TimeUnit, error) {
	unit := parquet.NewTimeUnit()
	switch unitStr {
	case "MILLIS":
		unit.MILLIS = parquet.NewMilliSeconds()
	case "MICROS":
		unit.MICROS = parquet.NewMicroSeconds()
	case "NANOS":
		unit.NANOS = parquet.NewNanoSeconds()
	default:
		return nil, fmt.Errorf("logicaltype time error, unknown unit: %s", unitStr)
	}
	return unit, nil
}

func newLogicalTypeFromFieldsMap(mp map[string]string) (*parquet.LogicalType, error) {
	val, ok := mp["logicaltype"]
	if !ok {
		return nil, errors.New("does not have logicaltype")
	}

	var err error
	logicalType := parquet.NewLogicalType()
	switch val {
	case "STRING":
		logicalType.STRING = parquet.NewStringType()
	case "MAP":
		logicalType.MAP = parquet.NewMapType()
	case "LIST":
		logicalType.LIST = parquet.NewListType()
	case "ENUM":
		logicalType.ENUM = parquet.NewEnumType()
	case "DECIMAL":
		logicalType.DECIMAL = parquet.NewDecimalType()
		if logicalType.DECIMAL.Precision, err = str2Int32(mp["logicaltype.precision"]); err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.precision as int32: %s", err.Error())
		}
		if logicalType.DECIMAL.Scale, err = str2Int32(mp["logicaltype.scale"]); err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.scale as int32: %s", err.Error())
		}
	case "DATE":
		logicalType.DATE = parquet.NewDateType()
	case "TIME":
		logicalType.TIME = parquet.NewTimeType()
		if logicalType.TIME.IsAdjustedToUTC, err = str2Bool(mp["logicaltype.isadjustedtoutc"]); err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.isadjustedtoutc as boolean: %s", err.Error())
		}
		if logicalType.TIME.Unit, err = newTimeUnitFromString(mp["logicaltype.unit"]); err != nil {
			return nil, err
		}
	case "TIMESTAMP":
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		if logicalType.TIMESTAMP.IsAdjustedToUTC, err = str2Bool(mp["logicaltype.isadjustedtoutc"]); err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.isadjustedtoutc as boolean: %s", err.Error())
		}
		if logicalType.TIMESTAMP.Unit, err = newTimeUnitFromString(mp["logicaltype.unit"]); err != nil {
			return nil, err
		}
	case "INTEGER":
		logicalType.INTEGER = parquet.NewIntType()
		bitWidth, err := str2Int32(mp["logicaltype.bitwidth"])
		if err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.bitwidth as int32: %s", err.Error())
		}
		logicalType.INTEGER.BitWidth = int8(bitWidth)
		if logicalType.INTEGER.IsSigned, err = str2Bool(mp["logicaltype.issigned"]); err != nil {
			return nil, fmt.Errorf("cannot parse logicaltype.issigned as boolean: %s", err.Error())
		}
	case "JSON":
		logicalType.JSON = parquet.NewJsonType()
	case "BSON":
		logicalType.BSON = parquet.NewBsonType()
	case "UUID":
		logicalType.UUID = parquet.NewUUIDType()
	default:
		return nil, fmt.Errorf("unknown logicaltype: %s", val)
	}

	return logicalType, nil
}

var intAttrMap = map[parquet.ConvertedType]struct {
	bitWidth int8
	isSigned bool
}{
	parquet.ConvertedType_INT_8:   {8, true},
	parquet.ConvertedType_INT_16:  {16, true},
	parquet.ConvertedType_INT_32:  {32, true},
	parquet.ConvertedType_INT_64:  {64, true},
	parquet.ConvertedType_UINT_8:  {8, false},
	parquet.ConvertedType_UINT_16: {16, false},
	parquet.ConvertedType_UINT_32: {32, false},
	parquet.ConvertedType_UINT_64: {64, false},
}

func newLogicalTypeFromConvertedType(schemaElement *parquet.SchemaElement, info *Tag) *parquet.LogicalType {
	if schemaElement.ConvertedType == nil {
		return nil
	}

	logicalType := parquet.NewLogicalType()
	if attr, ok := intAttrMap[*schemaElement.ConvertedType]; ok {
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = attr.bitWidth
		logicalType.INTEGER.IsSigned = attr.isSigned
		return logicalType
	}

	switch *schemaElement.ConvertedType {
	case parquet.ConvertedType_DECIMAL:
		logicalType.DECIMAL = parquet.NewDecimalType()
		logicalType.DECIMAL.Precision = info.Precision
		logicalType.DECIMAL.Scale = info.Scale
	case parquet.ConvertedType_DATE:
		logicalType.DATE = parquet.NewDateType()
	case parquet.ConvertedType_TIME_MICROS, parquet.ConvertedType_TIME_MILLIS:
		logicalType.TIME = parquet.NewTimeType()
		logicalType.TIME.IsAdjustedToUTC = info.isAdjustedToUTC
		logicalType.TIME.Unit, _ = newTimeUnitFromString(schemaElement.ConvertedType.String()[5:])
	case parquet.ConvertedType_TIMESTAMP_MICROS, parquet.ConvertedType_TIMESTAMP_MILLIS:
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		logicalType.TIMESTAMP.IsAdjustedToUTC = info.isAdjustedToUTC
		logicalType.TIMESTAMP.Unit, _ = newTimeUnitFromString(schemaElement.ConvertedType.String()[10:])
	case parquet.ConvertedType_BSON:
		logicalType.BSON = parquet.NewBsonType()
	case parquet.ConvertedType_ENUM:
		logicalType.ENUM = parquet.NewEnumType()
	case parquet.ConvertedType_JSON:
		logicalType.JSON = parquet.NewJsonType()
	case parquet.ConvertedType_LIST:
		logicalType.LIST = parquet.NewListType()
	case parquet.ConvertedType_MAP:
		logicalType.MAP = parquet.NewMapType()
	case parquet.ConvertedType_UTF8:
		logicalType.STRING = parquet.NewStringType()
	default:
		return nil
	}

	return logicalType
}

func DeepCopy(src, dst *Tag) {
	*dst = *src
	dst.logicalTypeFields = nil
	dst.Key.logicalTypeFields = nil
	dst.Value.logicalTypeFields = nil
}

// Get key tag map for map
func GetKeyTagMap(src *Tag) *Tag {
	res := NewTag()
	res.InName = "Key"
	res.ExName = "key"
	res.fieldAttr = src.Key
	res.logicalTypeFields = nil
	return res
}

// Get value tag map for map
func GetValueTagMap(src *Tag) *Tag {
	res := NewTag()
	res.InName = "Value"
	res.ExName = "value"
	res.fieldAttr = src.Value
	res.logicalTypeFields = nil
	return res
}

// Convert string to a golang variable name
func StringToVariableName(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	name := ""
	for i := range ln {
		c := str[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			name += string(c)
		} else {
			name += strconv.Itoa(int(c))
		}
	}

	name = headToUpper(name)
	return name
}

// Convert the first letter of a string to uppercase
func headToUpper(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	c := str[0]
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return strings.ToUpper(str[0:1]) + str[1:]
	}
	// handle non-alpha prefix such as "_"
	return "PARGO_PREFIX_" + str
}

func cmpIntBinary(as, bs, order string, signed bool) bool {
	abs := []byte(as)
	bbs := []byte(bs)
	la, lb := len(abs), len(bbs)

	// convert to big endian to simplify logic below
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
				for i := range lb - la {
					pre[i] = byte(0xFF)
				}
			}
			abs = append(pre, abs...)

		} else if la > lb {
			sb := (bbs[0] >> 7) & 1
			pre := make([]byte, la-lb)
			if sb == 1 {
				for i := range la - lb {
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

	for i := range abs {
		if abs[i] < bbs[i] {
			return true
		} else if abs[i] > bbs[i] {
			return false
		}
	}
	return false
}

func FindFuncTable(pT *parquet.Type, cT *parquet.ConvertedType, logT *parquet.LogicalType) FuncTable {
	if cT == nil && logT == nil {
		if *pT == parquet.Type_BOOLEAN {
			return boolFuncTable{}
		} else if *pT == parquet.Type_INT32 {
			return int32FuncTable{}
		} else if *pT == parquet.Type_INT64 {
			return int64FuncTable{}
		} else if *pT == parquet.Type_INT96 {
			return int96FuncTable{}
		} else if *pT == parquet.Type_FLOAT {
			return float32FuncTable{}
		} else if *pT == parquet.Type_DOUBLE {
			return float64FuncTable{}
		} else if *pT == parquet.Type_BYTE_ARRAY {
			return stringFuncTable{}
		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return stringFuncTable{}
		}
	}

	if cT != nil {
		if *cT == parquet.ConvertedType_UTF8 || *cT == parquet.ConvertedType_BSON || *cT == parquet.ConvertedType_JSON || *cT == parquet.ConvertedType_ENUM {
			return stringFuncTable{}
		} else if *cT == parquet.ConvertedType_INT_8 || *cT == parquet.ConvertedType_INT_16 || *cT == parquet.ConvertedType_INT_32 ||
			*cT == parquet.ConvertedType_DATE || *cT == parquet.ConvertedType_TIME_MILLIS {
			return int32FuncTable{}
		} else if *cT == parquet.ConvertedType_UINT_8 || *cT == parquet.ConvertedType_UINT_16 || *cT == parquet.ConvertedType_UINT_32 {
			return uint32FuncTable{}
		} else if *cT == parquet.ConvertedType_INT_64 || *cT == parquet.ConvertedType_TIME_MICROS ||
			*cT == parquet.ConvertedType_TIMESTAMP_MILLIS || *cT == parquet.ConvertedType_TIMESTAMP_MICROS {
			return int64FuncTable{}
		} else if *cT == parquet.ConvertedType_UINT_64 {
			return uint64FuncTable{}
		} else if *cT == parquet.ConvertedType_INTERVAL {
			return intervalFuncTable{}
		} else if *cT == parquet.ConvertedType_DECIMAL {
			if *pT == parquet.Type_BYTE_ARRAY || *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				return decimalStringFuncTable{}
			} else if *pT == parquet.Type_INT32 {
				return int32FuncTable{}
			} else if *pT == parquet.Type_INT64 {
				return int64FuncTable{}
			}
		}
	}

	if logT != nil {
		if logT.TIME != nil || logT.TIMESTAMP != nil {
			return FindFuncTable(pT, nil, nil)
		} else if logT.DATE != nil {
			return int32FuncTable{}
		} else if logT.INTEGER != nil {
			if logT.INTEGER.IsSigned {
				return FindFuncTable(pT, nil, nil)
			} else {
				if *pT == parquet.Type_INT32 {
					return uint32FuncTable{}
				} else if *pT == parquet.Type_INT64 {
					return uint64FuncTable{}
				}
			}
		} else if logT.DECIMAL != nil {
			if *pT == parquet.Type_BYTE_ARRAY || *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				return decimalStringFuncTable{}
			} else if *pT == parquet.Type_INT32 {
				return int32FuncTable{}
			} else if *pT == parquet.Type_INT64 {
				return int64FuncTable{}
			}
		} else if logT.BSON != nil || logT.JSON != nil || logT.STRING != nil || logT.UUID != nil {
			return stringFuncTable{}
		}
	}

	panic("No known func table in FindFuncTable")
}

func str2Int32(val string) (int32, error) {
	valInt, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}
	return int32(valInt), nil
}

func str2Bool(val string) (bool, error) {
	valBoolean, err := strconv.ParseBool(val)
	if err != nil {
		return false, err
	}
	return valBoolean, nil
}

type FuncTable interface {
	LessThan(a, b any) bool
	MinMaxSize(minVal, maxVal, val any) (any, any, int32)
}

func Min(table FuncTable, a, b any) any {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if table.LessThan(a, b) {
		return a
	} else {
		return b
	}
}

func Max(table FuncTable, a, b any) any {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if table.LessThan(a, b) {
		return b
	} else {
		return a
	}
}

type boolFuncTable struct{}

func (boolFuncTable) LessThan(a, b any) bool {
	return !a.(bool) && b.(bool)
}

func (table boolFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 1
}

type int32FuncTable struct{}

func (int32FuncTable) LessThan(a, b any) bool {
	return a.(int32) < b.(int32)
}

func (table int32FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type uint32FuncTable struct{}

func (uint32FuncTable) LessThan(a, b any) bool {
	return uint32(a.(int32)) < uint32(b.(int32))
}

func (table uint32FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type int64FuncTable struct{}

func (int64FuncTable) LessThan(a, b any) bool {
	return a.(int64) < b.(int64)
}

func (table int64FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type uint64FuncTable struct{}

func (uint64FuncTable) LessThan(a, b any) bool {
	return uint64(a.(int64)) < uint64(b.(int64))
}

func (table uint64FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type int96FuncTable struct{}

func (int96FuncTable) LessThan(ai, bi any) bool {
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
}

func (table int96FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type float32FuncTable struct{}

func (float32FuncTable) LessThan(a, b any) bool {
	return a.(float32) < b.(float32)
}

func (table float32FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type float64FuncTable struct{}

func (float64FuncTable) LessThan(a, b any) bool {
	return a.(float64) < b.(float64)
}

func (table float64FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type stringFuncTable struct{}

func (stringFuncTable) LessThan(a, b any) bool {
	return a.(string) < b.(string)
}

func (table stringFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type intervalFuncTable struct{}

func (intervalFuncTable) LessThan(ai, bi any) bool {
	a, b := []byte(ai.(string)), []byte(bi.(string))
	for i := 11; i >= 0; i-- {
		if a[i] > b[i] {
			return false
		} else if a[i] < b[i] {
			return true
		}
	}
	return false
}

func (table intervalFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type decimalStringFuncTable struct{}

func (decimalStringFuncTable) LessThan(a, b any) bool {
	return cmpIntBinary(a.(string), b.(string), "BigEndian", true)
}

func (table decimalStringFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

// Get the size of a parquet value
func SizeOf(val reflect.Value) int64 {
	if !val.IsValid() {
		return 0
	}
	var size int64
	switch val.Type().Kind() {
	case reflect.Ptr:
		if val.IsNil() {
			return 0
		}
		return SizeOf(val.Elem())
	case reflect.Slice:
		for i := range val.Len() {
			size += SizeOf(val.Index(i))
		}
		return size
	case reflect.Struct:
		for i := range val.Type().NumField() {
			size += SizeOf(val.Field(i))
		}
		return size
	case reflect.Map:
		keys := val.MapKeys()
		for i := range keys {
			size += SizeOf(keys[i])
			size += SizeOf(val.MapIndex(keys[i]))
		}
		return size
	case reflect.Bool:
		return 1
	case reflect.Int32:
		return 4
	case reflect.Int64:
		return 8
	case reflect.String:
		return int64(val.Len())
	case reflect.Float32:
		return 4
	case reflect.Float64:
		return 8
	}
	return 4
}

const PAR_GO_PATH_DELIMITER = "\x01"

// . -> \x01
func ReformPathStr(pathStr string) string {
	return strings.ReplaceAll(pathStr, ".", "\x01")
}

// Convert path slice to string
func PathToStr(path []string) string {
	return strings.Join(path, PAR_GO_PATH_DELIMITER)
}

// Convert string to path slice
func StrToPath(str string) []string {
	return strings.Split(str, PAR_GO_PATH_DELIMITER)
}

// Get the pathStr index in a path
func PathStrIndex(str string) int {
	return len(strings.Split(str, PAR_GO_PATH_DELIMITER))
}

func IsChildPath(parent, child string) bool {
	ln := len(parent)
	return strings.HasPrefix(child, parent) && (len(child) == ln || child[ln] == PAR_GO_PATH_DELIMITER[0])
}

// newTable creates empty table with transposed columns and records
func newTable(rowLen, colLen int) [][]any {
	tableLen := make([]any, rowLen*colLen)
	// Need to reconsinder to avoid allocation and memcopy.
	table := make([][]any, rowLen)
	lo, hi := 0, colLen
	for i := range table {
		table[i] = tableLen[lo:hi:hi]
		lo, hi = hi, hi+colLen
	}
	return table
}

// TransposeTable transposes a table's rows and columns once per arrow record.
// We need to transpose the rows and columns because parquet-go library writes
// data row by row while the arrow library provides the data column by column.
func TransposeTable(table [][]any) [][]any {
	transposedTable := newTable(len(table[0]), len(table))
	for i := range transposedTable {
		row := transposedTable[i]
		for j := range row {
			row[j] = table[j][i]
		}
	}
	return transposedTable
}

// ArrowColToParquetCol creates column with native go values from column
// with arrow values according to the rules described in the Type section in
// the project's README.md file.
//
// If `col` contains Null value but `field` is not marked as Nullable this
// results in an error.
func ArrowColToParquetCol(field arrow.Field, col arrow.Array) ([]any, error) {
	recs := make([]any, col.Len())
	switch field.Type.(type) {
	case *arrow.Int8Type:
		arr := col.(*array.Int8)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Int16Type:
		arr := col.(*array.Int16)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Int32Type:
		arr := col.(*array.Int32)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.Int64Type:
		arr := col.(*array.Int64)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.Uint8Type:
		arr := col.(*array.Uint8)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Uint16Type:
		arr := col.(*array.Uint16)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Uint32Type:
		arr := col.(*array.Uint32)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Uint64Type:
		arr := col.(*array.Uint64)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int64(arr.Value(i))
			}
		}
	case *arrow.Float32Type:
		arr := col.(*array.Float32)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.Float64Type:
		arr := col.(*array.Float64)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.Date32Type:
		arr := col.(*array.Date32)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Date64Type:
		arr := col.(*array.Date64)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.BinaryType:
		arr := col.(*array.Binary)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = string(arr.Value(i))
			}
		}
	case *arrow.StringType:
		arr := col.(*array.String)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.BooleanType:
		arr := col.(*array.Boolean)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.Time32Type:
		arr := col.(*array.Time32)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.TimestampType:
		arr := col.(*array.Timestamp)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int64(arr.Value(i))
			}
		}
	}
	return recs, nil
}

func nonNullableFieldContainsNullError(field arrow.Field, idx int) error {
	return fmt.Errorf("field with name '%s' is marked non-nullable but its "+
		"column array contains Null value at index %d", field.Name, idx)
}

func ToPtr[T any](value T) *T {
	return &value
}
