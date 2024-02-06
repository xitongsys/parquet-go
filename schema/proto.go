package schema

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ParquetFieldName = "name"

	ParquetType               = "type"
	ParquetConvertedType      = "convertedtype"
	ParquetValueType          = "valuetype"
	ParquetKeyType            = "keytype"
	ParquetKeyConvertedType   = "keyconvertedtype"
	ParquetValueConvertedType = "valueconvertedtype"

	ParquetTypeBoolean           = "BOOLEAN"
	ParquetTypeInt32             = "INT32"
	ParquetTypeInt64             = "INT64"
	ParquetTypeInt96             = "INT96"
	ParquetTypeFloat             = "FLOAT"
	ParquetTypeDouble            = "DOUBLE"
	ParquetTypeByteArray         = "BYTE_ARRAY"
	ParquetTypeFixedLenByteArray = "FIXED_LEN_BYTE_ARRAY"

	ParquetTypeMap    = "MAP"
	ParquetTypeList   = "LIST"
	ParquetTypeStruct = "STRUCT"

	ConvertedTypeUtf8            = "UTF8"
	ConvertedTypeMap             = "MAP"
	ConvertedTypeList            = "LIST"
	ConvertedTypeEnum            = "ENUM"
	ConvertedTypeDecimal         = "DECIMAL"
	ConvertedTypeDate            = "DATE"
	ConvertedTypeTimeMills       = "TIME_MILLIS"
	ConvertedTypeTimeMicros      = "TIME_MICROS"
	ConvertedTypeTimestampMills  = "TIMESTAMP_MILLIS"
	ConvertedTypeTimestampMicros = "TIMESTAMP_MICROS"
	ConvertedTypeUnit8           = "UINT_8"
	ConvertedTypeUnint16         = "UINT_16"
	ConvertedTypeUnint32         = "UINT_32"
	ConvertedTypeUnint64         = "UINT_64"
	ConvertedTypeInt8            = "INT_8"
	ConvertedTypeInt16           = "INT_16"
	ConvertedTypeInt32           = "INT_32"
	ConvertedTypeInt64           = "INT_64"
	ConvertedTypeJson            = "JSON"
	ConvertedTypeBson            = "BSON"
	ConvertedTypeInterval        = "INTERVAL"

	ProtoEnumMethodName   = "Enum"
	ProtoStringMethodName = "String"
)

var ProtoTimestampType = reflect.TypeOf(timestamppb.Timestamp{})

func IsPrimitiveParquetType(tp string) bool {
	switch tp {
	case ParquetTypeBoolean,
		ParquetTypeInt32,
		ParquetTypeInt64,
		ParquetTypeInt96,
		ParquetTypeFloat,
		ParquetTypeDouble,
		ParquetTypeByteArray,
		ParquetTypeFixedLenByteArray:
		return true
	default:
		return false
	}
}

func IsPrimitiveGoTypeKind(kind reflect.Kind) bool {
	switch kind {
	case
		reflect.Bool,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Int,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uint,
		reflect.Float32,
		reflect.Float64,
		reflect.String:
		return true
	default:
		return false
	}
}

func IsPointerGoTypeKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Uintptr, reflect.Interface:
		return true
	default:
		return false
	}
}

func IsPrimitiveOrPointerGoKind(kind reflect.Kind) bool {
	return IsPointerGoTypeKind(kind) || IsPrimitiveGoTypeKind(kind)
}

func IsProtoInternalField(name string) bool {
	switch name {
	case "state", "sizeCache", "unknownFields":
		return true
	default:
		return false

	}
}

func GetDefinedTypeTag(tp reflect.Type) (tags map[string]string, err error) {
	tags = make(map[string]string)

	switch tp.Kind() {
	case reflect.Bool:
		tags[ParquetType] = ParquetTypeBoolean
	case reflect.Int8:
		tags[ParquetType] = ParquetTypeInt32
		tags[ParquetConvertedType] = ConvertedTypeInt8
	case reflect.Int16:
		tags[ParquetType] = ParquetTypeInt32
		tags[ParquetConvertedType] = ConvertedTypeInt16
	case reflect.Int32:
		// speical handling for the proto enum generated constant, it checks Enum() method generated. It's not ideal but no better way to do it.
		if _, exists := tp.MethodByName(ProtoEnumMethodName); exists {
			tags[ParquetType] = ParquetTypeByteArray
			tags[ParquetConvertedType] = ConvertedTypeEnum
		} else {
			tags[ParquetType] = ParquetTypeInt32
		}
	case reflect.Int64:
		tags[ParquetType] = ParquetTypeInt64
	case reflect.Int:
		tags[ParquetType] = ParquetTypeInt64
	case reflect.Uint8:
		tags[ParquetType] = ParquetTypeInt32
		tags[ParquetConvertedType] = ConvertedTypeUnit8
	case reflect.Uint16:
		tags[ParquetType] = ParquetTypeInt32
		tags[ParquetConvertedType] = ConvertedTypeUnint16
	case reflect.Uint32:
		tags[ParquetType] = ParquetTypeInt32
		tags[ParquetConvertedType] = ConvertedTypeUnint32
	case reflect.Uint64:
		tags[ParquetType] = ParquetTypeInt64
		tags[ParquetConvertedType] = ConvertedTypeUnint64
	case reflect.Uint:
		tags[ParquetType] = ParquetTypeInt64
		tags[ParquetConvertedType] = ConvertedTypeUnint64
	case reflect.Float32:
		tags[ParquetType] = ParquetTypeFloat
	case reflect.Float64:
		tags[ParquetType] = ParquetTypeDouble
	case reflect.String:
		tags[ParquetType] = ParquetTypeByteArray
	case reflect.Pointer, reflect.UnsafePointer, reflect.Uintptr, reflect.Interface:
		tags, err = GetDefinedTypeTag(tp.Elem())
	case reflect.Array, reflect.Slice:
		tags, err = getListTag(tp)
	case reflect.Map:
		tags, err = getMapTag(tp)
	case reflect.Struct:
		// do nothing since the struct tag is not needed in schema generation
	default:
		return nil, fmt.Errorf("Type %s is not supported in generating tags.", tp)
	}
	return
}

// Generate the tag for the struct field when there is no predefined tag
func GenerateFieldTag(field reflect.StructField) (tagString string, err error) {
	tags, err := GetDefinedTypeTag(field.Type)
	if err != nil {
		return "", err
	}
	tags[ParquetFieldName] = field.Name
	return getTagStringFromTags(tags), nil
}

func getTagStringFromTags(tags map[string]string) string {
	var pairs []string
	for key, value := range tags {
		pairs = append(pairs, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(pairs, ", ")
}

func getMapTag(tp reflect.Type) (tags map[string]string, err error) {
	tags = make(map[string]string)
	tags[ParquetType] = ParquetTypeMap
	tags[ParquetConvertedType] = ParquetTypeMap
	if IsPrimitiveOrPointerGoKind(tp.Key().Kind()) {
		elementTags, elementErr := GetDefinedTypeTag(tp.Key())
		if elementErr != nil {
			return nil, elementErr
		}
		if elementType, exists := elementTags[ParquetType]; exists && IsPrimitiveParquetType(elementType) {
			tags[ParquetKeyType] = elementType
			if elementConvertedType, exists := elementTags[ParquetConvertedType]; exists {
				tags[ParquetKeyConvertedType] = elementConvertedType
			}
		}
	}
	if IsPrimitiveOrPointerGoKind(tp.Elem().Kind()) {
		elementTags, elementErr := GetDefinedTypeTag(tp.Elem())
		if elementErr != nil {
			return nil, elementErr
		}
		if elementType, exists := elementTags[ParquetType]; exists && IsPrimitiveParquetType(elementType) {
			tags[ParquetValueType] = elementType
			if elementConvertedType, exists := elementTags[ParquetConvertedType]; exists {
				tags[ParquetValueConvertedType] = elementConvertedType
			}
		}
	}
	return
}

func getListTag(tp reflect.Type) (tags map[string]string, err error) {
	tags = make(map[string]string)
	tags[ParquetType] = ParquetTypeList
	tags[ParquetConvertedType] = ParquetTypeList
	if IsPrimitiveOrPointerGoKind(tp.Elem().Kind()) {
		elementTags, elementErr := GetDefinedTypeTag(tp.Elem())
		if elementErr != nil {
			return nil, elementErr
		}
		if elementType, exists := elementTags[ParquetType]; exists && IsPrimitiveParquetType(elementType) {
			tags[ParquetValueType] = elementType
			if elementConvertedType, exists := elementTags[ParquetConvertedType]; exists {
				tags[ParquetValueConvertedType] = elementConvertedType
			}
		}
	}
	return
}

// Speical handling for the timestamp schema to convert it as millis as one int64 number instead of keeping it as struct.
func generateSchemaTimestamp(item *Item) (*parquet.SchemaElement, *common.Tag, error) {
	item.Info.Type = ParquetTypeInt64
	item.Info.ConvertedType = ConvertedTypeTimestampMills
	schema, err := common.NewSchemaElementFromTagMap(item.Info)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate schema for timestamp: %v", err)
	}
	newInfo := common.NewTag()
	common.DeepCopy(item.Info, newInfo)
	return schema, newInfo, nil
}

func NewSchemaHandlerFromProtoStruct(obj interface{}) (sh *SchemaHandler, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("error occurred")
			}
		}
	}()

	ot := reflect.TypeOf(obj)
	if ot.Kind() == reflect.Pointer {
		ot = ot.Elem()
	}
	item := NewItem()
	item.GoType = ot
	item.Info.InName = "Parquet_go_root"
	item.Info.ExName = "parquet_go_root"
	item.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED

	stack := make([]*Item, 1)
	stack[0] = item
	schemaElements := make([]*parquet.SchemaElement, 0)
	infos := make([]*common.Tag, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item = stack[ln-1]
		stack = stack[:ln-1]
		var newInfo *common.Tag

		if item.GoType.Kind() == reflect.Struct {
			if item.GoType == ProtoTimestampType {
				schema, newInfo, err := generateSchemaTimestamp(item)
				if err != nil {
					return nil, err
				}
				schemaElements = append(schemaElements, schema)
				infos = append(infos, newInfo)
				continue
			}
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info.InName
			schema.RepetitionType = &item.Info.RepetitionType
			numField := int32(item.GoType.NumField())
			schemaElements = append(schemaElements, schema)

			newInfo = common.NewTag()
			common.DeepCopy(item.Info, newInfo)
			numChildren := int32(0)
			infos = append(infos, newInfo)

			for i := int(numField - 1); i >= 0; i-- {
				f := item.GoType.Field(i)
				if IsProtoInternalField(f.Name) {
					continue
				}
				tagStr, err := GenerateFieldTag(f)
				if err != nil {
					return nil, err
				}
				newItem, err := generateSchemaForStructField(f, tagStr)
				if err != nil {
					return nil, err
				}
				stack = append(stack, newItem)
				numChildren++
			}
			schema.NumChildren = &numChildren
		} else if (item.GoType.Kind() == reflect.Slice || item.GoType.Kind() == reflect.Array) &&
			item.Info.RepetitionType != parquet.FieldRepetitionType_REPEATED {
			// special handling for the nested slice, the value type cannot be prvoided through the regular tag since it only has one layer deep
			if item.Info.ValueType == "" {
				tags, err := GetDefinedTypeTag(item.GoType.Elem())
				if err != nil {
					return nil, err
				}
				item.Info.ValueType = tags[ParquetType]
				item.Info.ValueConvertedType = tags[ParquetConvertedType]
			}
			stack, schemaElements, infos = generateSchemaForSlice(item, stack, schemaElements, infos)
		} else if item.GoType.Kind() == reflect.Slice &&
			item.Info.RepetitionType == parquet.FieldRepetitionType_REPEATED {
			newItem := NewItem()
			newItem.Info = item.Info
			newItem.GoType = item.GoType.Elem()
			stack = append(stack, newItem)
		} else if IsPointerGoTypeKind(item.GoType.Kind()) {
			item.GoType = item.GoType.Elem()
			stack = append(stack, item)
		} else if item.GoType.Kind() == reflect.Map {
			stack, schemaElements, infos = generateSchemaForMap(item, stack, schemaElements, infos)
		} else {
			if item.Info.Type == "" {
				tags, err := GetDefinedTypeTag(item.GoType)
				if err != nil {
					return nil, err
				}
				item.Info.Type = tags[ParquetType]
			}
			schema, err := common.NewSchemaElementFromTagMap(item.Info)
			if err != nil {
				return nil, fmt.Errorf("failed to create schema from tag map: %s", err.Error())
			}
			schemaElements = append(schemaElements, schema)
			newInfo = common.NewTag()
			common.DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)
		}
	}

	res := NewSchemaHandlerFromSchemaList(schemaElements)
	res.Infos = infos
	res.CreateInExMap()
	return res, nil
}
