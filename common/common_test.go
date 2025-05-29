package common

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_fieldAttr_update(t *testing.T) {
	testCases := map[string]struct {
		key, val string
		expected fieldAttr
		errMsg   string
	}{
		"type":                 {"type", "BOOLEAN", fieldAttr{Type: "BOOLEAN"}, ""},
		"convertedtype":        {"convertedtype", "UTF8", fieldAttr{convertedType: "UTF8"}, ""},
		"length-good":          {"length", "123", fieldAttr{Length: 123}, ""},
		"length-bad":           {"length", "abc", fieldAttr{}, "failed to parse length:"},
		"scale-good":           {"scale", "123", fieldAttr{Scale: 123}, ""},
		"scale-bad":            {"scale", "abc", fieldAttr{}, "failed to parse scale:"},
		"precision-good":       {"precision", "123", fieldAttr{Precision: 123}, ""},
		"precision-bad":        {"precision", "abc", fieldAttr{}, "failed to parse precision:"},
		"fieldid-good":         {"fieldid", "123", fieldAttr{fieldID: 123}, ""},
		"fieldid-bad":          {"fieldid", "abc", fieldAttr{}, "failed to parse fieldid:"},
		"isadjustedtoutc-good": {"isadjustedtoutc", "true", fieldAttr{isAdjustedToUTC: true}, ""},
		"isadjustedtoutc-bad":  {"isadjustedtoutc", "abc", fieldAttr{}, "failed to parse isadjustedtoutc:"},
		"omitstats-good":       {"omitstats", "true", fieldAttr{OmitStats: true}, ""},
		"omitstats-bad":        {"omitstats", "abc", fieldAttr{}, "failed to parse omitstats:"},
		"repetitiontype-good":  {"repetitiontype", "repeated", fieldAttr{RepetitionType: parquet.FieldRepetitionType_REPEATED}, ""},
		"repetitiontype-bad":   {"repetitiontype", "foobar", fieldAttr{}, "failed to parse repetitiontype:"},
		"encoding-good":        {"encoding", "plain", fieldAttr{Encoding: parquet.Encoding_PLAIN}, ""},
		"encoding-bad":         {"encoding", "foobar", fieldAttr{}, "failed to parse encoding:"},
		"logicaltype":          {"logicaltype.foo", "bar", fieldAttr{logicalTypeFields: map[string]string{"logicaltype.foo": "bar"}}, ""},
		"unknown-tag":          {"unknown-tag.foo", "foobar", fieldAttr{}, "unrecognized tag"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := fieldAttr{}
			err := actual.update(tc.key, tc.val)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_NewTag(t *testing.T) {
	actual := NewTag()
	require.NotNil(t, actual)
	require.Equal(t, Tag{}, *actual)
}

func Test_StringToTag(t *testing.T) {
	testCases := map[string]struct {
		tag      string
		expected Tag
		errMsg   string
	}{
		"missing=":         {" name ", Tag{}, "expect 'key=value' but got"},
		"name-only":        {"NAME = John", Tag{InName: "John", ExName: "John"}, ""},
		"inname-only":      {" inname = John ", Tag{InName: "John"}, ""},
		"name-then-inname": {" name=John,inname = Jane ", Tag{InName: "Jane", ExName: "John"}, ""},
		"inname-then-name": {" inname=John,name = Jane ", Tag{InName: "John", ExName: "Jane"}, ""},
		"tag-good":         {"type=BYTE_ARRAY,convertedtype=UTF8", Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY", convertedType: "UTF8"}}, ""},
		"tag-bad":          {"foo=bar", Tag{}, "failed to parse tag"},
		"key-good":         {"keytype=INT32,KeyConvertedtype=TIME", Tag{Key: fieldAttr{Type: "INT32", convertedType: "TIME"}}, ""},
		"key-bad":          {"keyfoo=bar", Tag{}, "failed to parse tag"},
		"value-good":       {"valuetype=INT32, valuerepetitiontype=REPEATED", Tag{Value: fieldAttr{Type: "INT32", RepetitionType: parquet.FieldRepetitionType_REPEATED}}, ""},
		"value-bad":        {"valuefoo=bar", Tag{}, "failed to parse tag"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := StringToTag(tc.tag)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, *actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_NewSchemaElementFromTagMap(t *testing.T) {
	testCases := map[string]struct {
		tag      Tag
		expected parquet.SchemaElement
		errMsg   string
	}{
		"missing-type": {Tag{}, parquet.SchemaElement{}, "not a valid Type string"},
		"logicaltype-bad": {
			Tag{
				fieldAttr: fieldAttr{
					Type:              "BYTE_ARRAY",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
			},
			parquet.SchemaElement{},
			"failed to create logicaltype from field map",
		},
		"all-good": {
			Tag{
				fieldAttr: fieldAttr{
					Type:           "BYTE_ARRAY",
					convertedType:  "UTF8",
					RepetitionType: parquet.FieldRepetitionType_REQUIRED,
					Length:         10,
					Scale:          9,
					Precision:      8,
					fieldID:        7,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_BYTE_ARRAY),
				TypeLength:     ToPtr(int32(10)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(9)),
				Precision:      ToPtr(int32(8)),
				FieldID:        ToPtr(int32(7)),
				ConvertedType:  ToPtr(parquet.ConvertedType_UTF8),
				LogicalType:    ToPtr(parquet.LogicalType{STRING: &parquet.StringType{}}),
			},
			"",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := NewSchemaElementFromTagMap(&tc.tag)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, *actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_newTimeUnitFromString(t *testing.T) {
	testCases := map[string]struct {
		unit     string
		expected parquet.TimeUnit
		errMsg   string
	}{
		"MILLIS": {"MILLIS", parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}, ""},
		"MICROS": {"MICROS", parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}, ""},
		"NANOS":  {"NANOS", parquet.TimeUnit{NANOS: parquet.NewNanoSeconds()}, ""},
		"foobar": {"foobar", parquet.TimeUnit{}, "logicaltype time error, unknown unit:"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := newTimeUnitFromString(tc.unit)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, *actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_newLogicalTypeFromFieldsMap(t *testing.T) {
	testCases := map[string]struct {
		fields   map[string]string
		expected parquet.LogicalType
		errMsg   string
	}{
		"missing-logicaltype": {map[string]string{}, parquet.LogicalType{}, "does not have logicaltype"},
		"string": {
			map[string]string{"logicaltype": "STRING"},
			parquet.LogicalType{STRING: &parquet.StringType{}},
			"",
		},
		"list": {
			map[string]string{"logicaltype": "LIST"},
			parquet.LogicalType{LIST: &parquet.ListType{}},
			"",
		},
		"map": {
			map[string]string{"logicaltype": "MAP"},
			parquet.LogicalType{MAP: &parquet.MapType{}},
			"",
		},
		"enum": {
			map[string]string{"logicaltype": "ENUM"},
			parquet.LogicalType{ENUM: &parquet.EnumType{}},
			"",
		},
		"date": {
			map[string]string{"logicaltype": "DATE"},
			parquet.LogicalType{DATE: &parquet.DateType{}},
			"",
		},
		"json": {
			map[string]string{"logicaltype": "JSON"},
			parquet.LogicalType{JSON: &parquet.JsonType{}},
			"",
		},
		"bson": {
			map[string]string{"logicaltype": "BSON"},
			parquet.LogicalType{BSON: &parquet.BsonType{}},
			"",
		},
		"uuid": {
			map[string]string{"logicaltype": "UUID"},
			parquet.LogicalType{UUID: &parquet.UUIDType{}},
			"",
		},
		"decimal-bad-precision": {
			map[string]string{"logicaltype": "DECIMAL"},
			parquet.LogicalType{DECIMAL: &parquet.DecimalType{}},
			"cannot parse logicaltype.precision as int32",
		},
		"decimal-bad-scale": {
			map[string]string{"logicaltype": "DECIMAL", "logicaltype.precision": "10"},
			parquet.LogicalType{DECIMAL: &parquet.DecimalType{}},
			"cannot parse logicaltype.scale as int32",
		},
		"decimal-good": {
			map[string]string{"logicaltype": "DECIMAL", "logicaltype.precision": "10", "logicaltype.scale": "2"},
			parquet.LogicalType{DECIMAL: &parquet.DecimalType{Precision: 10, Scale: 2}},
			"",
		},
		"time-bad-adjustutc": {
			map[string]string{"logicaltype": "TIME"},
			parquet.LogicalType{TIME: &parquet.TimeType{}},
			"cannot parse logicaltype.isadjustedtoutc as bool",
		},
		"time-bad-unit": {
			map[string]string{"logicaltype": "TIME", "logicaltype.isadjustedtoutc": "true"},
			parquet.LogicalType{TIME: &parquet.TimeType{}},
			"logicaltype time error, unknown unit:",
		},
		"time-good": {
			map[string]string{"logicaltype": "TIME", "logicaltype.isadjustedtoutc": "true", "logicaltype.unit": "MILLIS"},
			parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			"",
		},
		"timestamp-bad-adjustutc": {
			map[string]string{"logicaltype": "TIMESTAMP"},
			parquet.LogicalType{TIME: &parquet.TimeType{}},
			"cannot parse logicaltype.isadjustedtoutc as bool",
		},
		"timestamp-bad-unit": {
			map[string]string{"logicaltype": "TIMESTAMP", "logicaltype.isadjustedtoutc": "true"},
			parquet.LogicalType{TIME: &parquet.TimeType{}},
			"logicaltype time error, unknown unit:",
		},
		"timestamp-good": {
			map[string]string{"logicaltype": "TIMESTAMP", "logicaltype.isadjustedtoutc": "true", "logicaltype.unit": "MILLIS"},
			parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			"",
		},
		"integer-bad-bitwidth": {
			map[string]string{"logicaltype": "INTEGER"},
			parquet.LogicalType{INTEGER: &parquet.IntType{}},
			"cannot parse logicaltype.bitwidth as int32",
		},
		"integer-bad-signed": {
			map[string]string{"logicaltype": "INTEGER", "logicaltype.bitwidth": "64"},
			parquet.LogicalType{INTEGER: &parquet.IntType{}},
			"cannot parse logicaltype.issigned as boolean:",
		},
		"integer-good": {
			map[string]string{"logicaltype": "INTEGER", "logicaltype.bitwidth": "64", "logicaltype.issigned": "true"},
			parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: true}},
			"",
		},
		"bad-logicaltype": {
			map[string]string{"logicaltype": "foobar"},
			parquet.LogicalType{STRING: &parquet.StringType{}},
			"unknown logicaltype:",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := newLogicalTypeFromFieldsMap(tc.fields)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, *actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_newLogicalTypeFromConvertedType(t *testing.T) {
	testCases := map[string]struct {
		schema   parquet.SchemaElement
		tag      Tag
		expected *parquet.LogicalType
	}{
		"nil-schema": {parquet.SchemaElement{}, Tag{}, nil},
		"int8": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_INT_8)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 8, IsSigned: true}},
		},
		"int16": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_INT_16)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 16, IsSigned: true}},
		},
		"int32": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_INT_32)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: true}},
		},
		"int64": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_INT_64)},
			Tag{fieldAttr: fieldAttr{Type: "INT64"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: true}},
		},
		"uint8": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_UINT_8)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 8, IsSigned: false}},
		},
		"uint16": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_UINT_16)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 16, IsSigned: false}},
		},
		"uint32": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_UINT_32)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: false}},
		},
		"uint64": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_UINT_64)},
			Tag{fieldAttr: fieldAttr{Type: "INT64"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: false}},
		},
		"decimal": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_DECIMAL)},
			Tag{fieldAttr: fieldAttr{Type: "INT32", Precision: 10, Scale: 9}},
			&parquet.LogicalType{DECIMAL: &parquet.DecimalType{Precision: 10, Scale: 9}},
		},
		"date": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_DATE)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{DATE: &parquet.DateType{}},
		},
		"time-millis": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_TIME_MILLIS)},
			Tag{fieldAttr: fieldAttr{Type: "INT64", isAdjustedToUTC: true}},
			&parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
		},
		"time-micros": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_TIME_MICROS)},
			Tag{fieldAttr: fieldAttr{Type: "INT64", isAdjustedToUTC: false}},
			&parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: false, Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}},
		},
		"timestamp-millis": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_TIMESTAMP_MILLIS)},
			Tag{fieldAttr: fieldAttr{Type: "INT64", isAdjustedToUTC: true}},
			&parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
		},
		"timestamp-micros": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_TIMESTAMP_MICROS)},
			Tag{fieldAttr: fieldAttr{Type: "INT64", isAdjustedToUTC: false}},
			&parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: false, Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}},
		},
		"bson": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_BYTE_ARRAY), ConvertedType: ToPtr(parquet.ConvertedType_BSON)},
			Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY"}},
			&parquet.LogicalType{BSON: &parquet.BsonType{}},
		},
		"enum": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_ENUM)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{ENUM: &parquet.EnumType{}},
		},
		"json": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_BYTE_ARRAY), ConvertedType: ToPtr(parquet.ConvertedType_JSON)},
			Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY"}},
			&parquet.LogicalType{JSON: &parquet.JsonType{}},
		},
		"list": {
			parquet.SchemaElement{Type: nil, ConvertedType: ToPtr(parquet.ConvertedType_LIST)},
			Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY"}},
			&parquet.LogicalType{LIST: &parquet.ListType{}},
		},
		"map": {
			parquet.SchemaElement{Type: nil, ConvertedType: ToPtr(parquet.ConvertedType_MAP)},
			Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY"}},
			&parquet.LogicalType{MAP: &parquet.MapType{}},
		},
		"utf8": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_BYTE_ARRAY), ConvertedType: ToPtr(parquet.ConvertedType_UTF8)},
			Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY"}},
			&parquet.LogicalType{STRING: &parquet.StringType{}},
		},
		"interval": {
			parquet.SchemaElement{Type: nil, ConvertedType: ToPtr(parquet.ConvertedType_INTERVAL)},
			Tag{fieldAttr: fieldAttr{}},
			nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := newLogicalTypeFromConvertedType(&tc.schema, &tc.tag)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func Test_DeepCopy(t *testing.T) {
	testCases := map[string]struct {
		src      Tag
		expected Tag
	}{
		"empty": {Tag{}, Tag{}},
		"with-logicaltype": {
			Tag{
				InName: "inname",
				ExName: "exname",
				fieldAttr: fieldAttr{
					Type:              "BOOLEAN",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
				Key: fieldAttr{
					Type:              "BYTE_ARRAY",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
				Value: fieldAttr{
					Type:              "INT32",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
			},
			Tag{
				InName: "inname",
				ExName: "exname",
				fieldAttr: fieldAttr{
					Type: "BOOLEAN",
				},
				Key: fieldAttr{
					Type: "BYTE_ARRAY",
				},
				Value: fieldAttr{
					Type: "INT32",
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			dst := NewTag()
			DeepCopy(&tc.src, dst)
			require.Equal(t, tc.expected, *dst)
		})
	}
}

func Test_GetKeyTagMap(t *testing.T) {
	testCases := map[string]struct {
		src      Tag
		expected Tag
	}{
		"empty": {Tag{}, Tag{InName: "Key", ExName: "key"}},
		"with-logicaltype": {
			Tag{
				Key: fieldAttr{
					Type:              "UNT32",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
			},
			Tag{
				InName: "Key",
				ExName: "key",
				fieldAttr: fieldAttr{
					Type: "UNT32",
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			dst := GetKeyTagMap(&tc.src)
			require.Equal(t, tc.expected, *dst)
		})
	}
}

func Test_GetValueTagMap(t *testing.T) {
	testCases := map[string]struct {
		src      Tag
		expected Tag
	}{
		"empty": {Tag{}, Tag{InName: "Value", ExName: "value"}},
		"with-logicaltype": {
			Tag{
				Value: fieldAttr{
					Type:              "UNT32",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
			},
			Tag{
				InName: "Value",
				ExName: "value",
				fieldAttr: fieldAttr{
					Type: "UNT32",
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			dst := GetValueTagMap(&tc.src)
			require.Equal(t, tc.expected, *dst)
		})
	}
}

func Test_StringToVariableName(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected string
	}{
		"empty":        {"", ""},
		"invalid-char": {"!@#", "PARGO_PREFIX_336435"},
		"no-change":    {"Name", "Name"},
		"title":        {"name", "Name"},
		"prefix":       {"12", "PARGO_PREFIX_12"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			varName := StringToVariableName(tc.str)
			require.Equal(t, tc.expected, varName)
		})
	}
}

func Test_headToUpper(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected string
	}{
		"empty":          {"", ""},
		"lowercase":      {"hello", "Hello"},
		"uppercase":      {"HeHH", "HeHH"},
		"not-alphabetic": {"123", "PARGO_PREFIX_123"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := headToUpper(tc.str)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func Test_cmpIntBinary(t *testing.T) {
	testCases := map[string]struct {
		a        []byte
		b        []byte
		endian   string
		signed   bool
		expected bool
	}{
		"8-bits: 0 < 0":        {[]byte{0}, []byte{0}, "LittleEndian", true, false},
		"8-bits: 0 < -1":       {[]byte{0}, []byte{255}, "LittleEndian", true, false},
		"8-bits: -1 < 0":       {[]byte{255}, []byte{0}, "LittleEndian", true, true},
		"8-bits: 255 < 0":      {[]byte{255}, []byte{0}, "LittleEndian", false, false},
		"16-bits: -1 < 0":      {[]byte{255, 255}, []byte{0, 0}, "LittleEndian", true, true},
		"16-bits: 65535 < 0":   {[]byte{255, 255}, []byte{0, 0}, "LittleEndian", false, false},
		"16-bits: -256 < 0":    {[]byte{255, 0}, []byte{0, 0}, "BigEndian", true, true},
		"16-bits: 65280 < 0":   {[]byte{0, 255}, []byte{0, 0}, "LittleEndian", false, false},
		"8/16-bits: -1 < -2":   {[]byte{255}, []byte{255, 254}, "BigEndian", true, false},
		"16/8-bits: -2 < -1":   {[]byte{254, 255}, []byte{255}, "LittleEndian", true, true},
		"8/16-bits: 255 < 254": {[]byte{255}, []byte{0, 254}, "BigEndian", false, false},
		"16/8-bits: 254 < 255": {[]byte{254, 0}, []byte{255}, "LittleEndian", false, true},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, cmpIntBinary(string(tc.a), string(tc.b), tc.endian, tc.signed), tc.expected)
		})
	}
}

func Test_FindFuncTable(t *testing.T) {
	testCases := map[string]struct {
		pT       *parquet.Type
		cT       *parquet.ConvertedType
		lT       *parquet.LogicalType
		expected FuncTable
	}{
		"BOOLEAN-nil-nil":                   {ToPtr(parquet.Type_BOOLEAN), nil, nil, boolFuncTable{}},
		"INT32-nil-nil":                     {ToPtr(parquet.Type_INT32), nil, nil, int32FuncTable{}},
		"INT64-nil-nil":                     {ToPtr(parquet.Type_INT64), nil, nil, int64FuncTable{}},
		"INT96-nil-nil":                     {ToPtr(parquet.Type_INT96), nil, nil, int96FuncTable{}},
		"FLOAT-nil-nil":                     {ToPtr(parquet.Type_FLOAT), nil, nil, float32FuncTable{}},
		"DOUBLE-nil-nil":                    {ToPtr(parquet.Type_DOUBLE), nil, nil, float64FuncTable{}},
		"BYTE_ARRAY-nil-nil":                {ToPtr(parquet.Type_BYTE_ARRAY), nil, nil, stringFuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-nil-nil":      {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, nil, stringFuncTable{}},
		"BYTE_ARRAY-UTF8-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_UTF8), nil, stringFuncTable{}},
		"BYTE_ARRAY-BSON-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_BSON), nil, stringFuncTable{}},
		"BYTE_ARRAY-JSON-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_JSON), nil, stringFuncTable{}},
		"BYTE_ARRAY-ENUM-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_ENUM), nil, stringFuncTable{}},
		"INT32-INT_8-nil":                   {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_INT_8), nil, int32FuncTable{}},
		"INT32-INT_16-nil":                  {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_INT_16), nil, int32FuncTable{}},
		"INT32-INT_32-nil":                  {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_INT_32), nil, int32FuncTable{}},
		"INT64-INT_64-nil":                  {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_INT_64), nil, int64FuncTable{}},
		"INT32-UINT_8-nil":                  {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_UINT_8), nil, uint32FuncTable{}},
		"INT32-UINT_16-nil":                 {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_UINT_16), nil, uint32FuncTable{}},
		"INT32-UINT_32-nil":                 {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_UINT_32), nil, uint32FuncTable{}},
		"INT64-UINT_64-nil":                 {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_UINT_64), nil, uint64FuncTable{}},
		"INT32-DATE-nil":                    {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_DATE), nil, int32FuncTable{}},
		"INT64-TIME_MILLIS-nil":             {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_TIME_MILLIS), nil, int32FuncTable{}},
		"INT64-TIME_MICROS-nil":             {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_TIME_MICROS), nil, int64FuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-INTERVAL-nil": {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), ToPtr(parquet.ConvertedType_INTERVAL), nil, intervalFuncTable{}},
		"BYTE_ARRAY-DECIMAL-nil":            {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_DECIMAL), nil, decimalStringFuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-DECIMAL-nil":  {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), ToPtr(parquet.ConvertedType_DECIMAL), nil, decimalStringFuncTable{}},
		"INT32-DECIMAL-nil":                 {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_DECIMAL), nil, int32FuncTable{}},
		"INT64-DECIMAL-nil":                 {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_DECIMAL), nil, int64FuncTable{}},
		"INT32-nil-TIME":                    {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{TIME: &parquet.TimeType{}}, int32FuncTable{}},
		"INT64-nil-TIMESTAMP":               {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{TIME: &parquet.TimeType{}}, int64FuncTable{}},
		"INT64-nil-DATE":                    {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{DATE: &parquet.DateType{}}, int32FuncTable{}},
		"INT32-nil-INTEGER-signed":          {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{IsSigned: true}}, int32FuncTable{}},
		"INT64-nil-INTEGER-signed":          {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{IsSigned: true}}, int64FuncTable{}},
		"INT32-nil-INTEGER-unsigned":        {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{}}, uint32FuncTable{}},
		"INT64-nil-INTEGER-unsigned":        {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{}}, uint64FuncTable{}},
		"BYTE_ARRAY-nil-DECIMAL":            {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, decimalStringFuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-nil-DECIMAL":  {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, decimalStringFuncTable{}},
		"INT32-nil-DECIMAL":                 {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, int32FuncTable{}},
		"INT64-nil-DECIMAL":                 {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, int64FuncTable{}},
		"BYTE_ARRAY-nil-BSON":               {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{BSON: &parquet.BsonType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-JSON":               {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{JSON: &parquet.JsonType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-STRING":             {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{STRING: &parquet.StringType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-UUID":               {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{UUID: &parquet.UUIDType{}}, stringFuncTable{}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, FindFuncTable(tc.pT, tc.cT, tc.lT))
		})
	}

	t.Run("panic", func(t *testing.T) {
		require.Panics(t, func() { FindFuncTable(nil, nil, &parquet.LogicalType{}) })
	})
}

func Test_str2Int32(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected int32
		errMsg   string
	}{
		"bad":  {"abc", 0, "strconv.Atoi: parsing"},
		"good": {"123", 123, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := str2Int32(tc.str)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_str2Bool(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected bool
		errMsg   string
	}{
		"bad":  {"abc", false, "strconv.ParseBool: parsing"},
		"good": {"true", true, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := str2Bool(tc.str)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_Min(t *testing.T) {
	testCases := map[string]struct {
		Num1, Num2 any
		PT         *parquet.Type
		CT         *parquet.ConvertedType
		Expected   any
	}{
		"nil-int32":     {nil, int32(1), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		"nil-nil":       {nil, nil, parquet.TypePtr(parquet.Type_INT32), nil, nil},
		"int32-nil":     {int32(1), nil, parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		"int32-int32-1": {int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		"int32-int32-2": {int32(2), int32(1), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			funcTable := FindFuncTable(tc.PT, tc.CT, nil)
			res := Min(funcTable, tc.Num1, tc.Num2)
			if res != tc.Expected {
				t.Errorf("Min err, expect %v, get %v", tc.Expected, res)
			}
		})
	}
}

func Test_Max(t *testing.T) {
	testCases := map[string]struct {
		Num1, Num2 any
		PT         *parquet.Type
		CT         *parquet.ConvertedType
		Expected   any
	}{
		"nil-int32":     {nil, int32(1), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		"nil-nil":       {nil, nil, parquet.TypePtr(parquet.Type_INT32), nil, nil},
		"int32-nil":     {int32(1), nil, parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		"int32-int32-1": {int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), nil, int32(2)},
		"int32-int32-2": {int32(2), int32(1), parquet.TypePtr(parquet.Type_INT32), nil, int32(2)},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			funcTable := FindFuncTable(tc.PT, tc.CT, nil)
			res := Max(funcTable, tc.Num1, tc.Num2)
			if res != tc.Expected {
				t.Errorf("Max err, expect %v, get %v", tc.Expected, res)
			}
		})
	}
}

func Test_LessThan(t *testing.T) {
	toHex := func(input string) string {
		ret, _ := hex.DecodeString(input)
		return string(ret)
	}
	testCases := map[string]struct {
		f        FuncTable
		a        any
		b        any
		expected bool
	}{
		"bool":       {boolFuncTable{}, true, false, false},
		"int32":      {int32FuncTable{}, int32(1), int32(2), true},
		"uint32":     {uint32FuncTable{}, int32(1), int32(2), true},
		"int64":      {int64FuncTable{}, int64(1), int64(2), true},
		"uint64":     {uint64FuncTable{}, int64(1), int64(2), true},
		"int96-1":    {int96FuncTable{}, toHex("000000000000000000000001"), toHex("010000000000000000000000"), false},
		"int96-2":    {int96FuncTable{}, toHex("000000000000000000010000"), toHex("000000000000000000020000"), true},
		"int96-3":    {int96FuncTable{}, toHex("0000000000000000000000ff"), toHex("010000000000000000000000"), true},
		"int96-4":    {int96FuncTable{}, toHex("000000000000000000000000"), toHex("0100000000000000000000ff"), false},
		"int96-5":    {int96FuncTable{}, toHex("000000000000000000000000"), toHex("000000000000000000000000"), false},
		"float32":    {float32FuncTable{}, float32(1), float32(2), true},
		"float64":    {float64FuncTable{}, float64(1), float64(2), true},
		"string":     {stringFuncTable{}, "a", "b", true},
		"interval-1": {intervalFuncTable{}, toHex("000000000000000000000001"), toHex("010000000000000000000000"), false},
		"interval-2": {intervalFuncTable{}, toHex("000000000000000000000001"), toHex("000000000000000000000002"), true},
		"interval-3": {intervalFuncTable{}, toHex("000000000000000000000000"), toHex("000000000000000000000000"), false},
		"decimal":    {decimalStringFuncTable{}, "\x00\x02", "\x00\x01", false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.f.LessThan(tc.a, tc.b))
		})
	}
}

func Test_MinMaxSize(t *testing.T) {
	toHex := func(input string) string {
		ret, _ := hex.DecodeString(input)
		return string(ret)
	}
	testCases := map[string]struct {
		f            FuncTable
		minVal       any
		maxVal       any
		val          any
		expectedMin  any
		expectedMax  any
		expectedSize int32
	}{
		"bool-1":   {boolFuncTable{}, false, true, false, false, true, 1},
		"bool-2":   {boolFuncTable{}, false, true, true, false, true, 1},
		"bool-3":   {boolFuncTable{}, false, false, true, false, true, 1},
		"bool-4":   {boolFuncTable{}, true, true, false, false, true, 1},
		"int32-1":  {int32FuncTable{}, int32(2), int32(4), int32(1), int32(1), int32(4), 4},
		"int32-2":  {int32FuncTable{}, int32(2), int32(4), int32(3), int32(2), int32(4), 4},
		"int32-3":  {int32FuncTable{}, int32(2), int32(4), int32(5), int32(2), int32(5), 4},
		"uint32-1": {uint32FuncTable{}, int32(2), int32(4), int32(1), int32(1), int32(4), 4},
		"uint32-2": {uint32FuncTable{}, int32(2), int32(4), int32(3), int32(2), int32(4), 4},
		"uint32-3": {uint32FuncTable{}, int32(2), int32(4), int32(5), int32(2), int32(5), 4},
		"int64-1":  {int64FuncTable{}, int64(2), int64(4), int64(1), int64(1), int64(4), 8},
		"int64-2":  {int64FuncTable{}, int64(2), int64(4), int64(3), int64(2), int64(4), 8},
		"int64-3":  {int64FuncTable{}, int64(2), int64(4), int64(5), int64(2), int64(5), 8},
		"uint64-1": {uint64FuncTable{}, int64(2), int64(4), int64(1), int64(1), int64(4), 8},
		"uint64-2": {uint64FuncTable{}, int64(2), int64(4), int64(3), int64(2), int64(4), 8},
		"uint64-3": {uint64FuncTable{}, int64(2), int64(4), int64(5), int64(2), int64(5), 8},
		"int96-1": {
			int96FuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000001"),
			toHex("000000000000000000000001"),
			toHex("000000000000000000000004"),
			12,
		},
		"int96-2": {
			int96FuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000003"),
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			12,
		},
		"int96-3": {
			int96FuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000005"),
			toHex("000000000000000000000002"),
			toHex("000000000000000000000005"),
			12,
		},
		"float32-1": {float32FuncTable{}, float32(2), float32(4), float32(1), float32(1), float32(4), 4},
		"float32-2": {float32FuncTable{}, float32(2), float32(4), float32(3), float32(2), float32(4), 4},
		"float32-3": {float32FuncTable{}, float32(2), float32(4), float32(5), float32(2), float32(5), 4},
		"float64-1": {float64FuncTable{}, float64(2), float64(4), float64(1), float64(1), float64(4), 8},
		"float64-2": {float64FuncTable{}, float64(2), float64(4), float64(3), float64(2), float64(4), 8},
		"float64-3": {float64FuncTable{}, float64(2), float64(4), float64(5), float64(2), float64(5), 8},
		"string-1":  {stringFuncTable{}, "2", "4", "11", "11", "4", 2},
		"string-2":  {stringFuncTable{}, "2", "4", "33", "2", "4", 2},
		"string-3":  {stringFuncTable{}, "2", "4", "55", "2", "55", 2},
		"interval-1": {
			intervalFuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000001"),
			toHex("000000000000000000000001"),
			toHex("000000000000000000000004"),
			12,
		},
		"interval-2": {
			intervalFuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000003"),
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			12,
		},
		"interval-3": {
			intervalFuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000005"),
			toHex("000000000000000000000002"),
			toHex("000000000000000000000005"),
			12,
		},
		"decimal-1": {decimalStringFuncTable{}, "\x02", "\x04", "\x00\x01", "\x00\x01", "\x04", 2},
		"decimal-2": {decimalStringFuncTable{}, "\x02", "\x04", "\x00\x03", "\x02", "\x04", 2},
		"decimal-3": {decimalStringFuncTable{}, "\x02", "\x04", "\x00\x05", "\x02", "\x00\x05", 2},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			newMin, newMax, size := tc.f.MinMaxSize(tc.minVal, tc.maxVal, tc.val)
			require.Equal(t, tc.expectedMin, newMin)
			require.Equal(t, tc.expectedMax, newMax)
			require.Equal(t, tc.expectedSize, int32(size))
		})
	}
}

func Test_SizeOf(t *testing.T) {
	testCases := map[string]struct {
		val      reflect.Value
		expected int64
	}{
		"bool":            {reflect.ValueOf(true), 1},
		"bool-pointer":    {reflect.ValueOf(ToPtr(true)), 1},
		"int32":           {reflect.ValueOf(int32(1)), 4},
		"int32-pointer":   {reflect.ValueOf(ToPtr(int32(1))), 4},
		"int64":           {reflect.ValueOf(int64(1)), 8},
		"int64-pointer":   {reflect.ValueOf(ToPtr(int64(1))), 8},
		"string":          {reflect.ValueOf("012345678901"), 12},
		"string-empty":    {reflect.ValueOf(""), 0},
		"string-pointer":  {reflect.ValueOf(ToPtr("012345678901")), 12},
		"float32":         {reflect.ValueOf(float32(0.1)), 4},
		"float32-pointer": {reflect.ValueOf(ToPtr(float32(0.1))), 4},
		"float64":         {reflect.ValueOf(float64(0.1)), 8},
		"float64-pointer": {reflect.ValueOf(ToPtr(float64(0.1))), 8},
		"pointer-nil":     {reflect.ValueOf((*string)(nil)), 0},
		"slice":           {reflect.ValueOf([]int32{1, 2, 3}), 12},
		"map": {reflect.ValueOf(map[string]int32{
			string("1"):   1,
			string("11"):  11,
			string("111"): 111,
		}), 18},
		"struct": {reflect.ValueOf(struct {
			A int32
			B int64
			C []string
			D map[string]string
		}{
			1, 2,
			[]string{"hello", "world", "", "good"},
			map[string]string{
				string("hello"): string("012345678901"),
				string("world"): string("012345678901"),
			},
		}), 60},
		"channel":       {reflect.ValueOf(make(chan int)), 4},
		"invalid-value": {reflect.ValueOf(nil), 0},
	}

	for _, data := range testCases {
		res := SizeOf(data.val)
		if res != data.expected {
			t.Errorf("SizeOf err, expect %v, get %v", data.expected, res)
		}
	}
}

func Test_ReformPathStr(t *testing.T) {
	testCases := map[string]struct {
		path     string
		expected string
	}{
		"test-case-1": {"a.b.c", "a\x01b\x01c"},
		"test-case-2": {"a..c", "a\x01\x01c"},
		"test-case-3": {"", ""},
		"test-case-4": {"abc", "abc"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, ReformPathStr(tc.path))
		})
	}
}

func Test_PathToStr(t *testing.T) {
	testCases := map[string]struct {
		path     []string
		expected string
	}{
		"test-case-1": {[]string{"a", "b", "c"}, "a\x01b\x01c"},
		"test-case-2": {[]string{"a", "", "c"}, "a\x01\x01c"},
		"test-case-3": {[]string{}, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, PathToStr(tc.path))
		})
	}
}

func Test_StrToPath(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected []string
	}{
		"test-case-1": {"a\x01b\x01c", []string{"a", "b", "c"}},
		"test-case-2": {"a\x01\x01c", []string{"a", "", "c"}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, StrToPath(tc.str))
		})
	}
}

func Test_PathStrIndex(t *testing.T) {
	testCases := map[string]struct {
		path     string
		expected int
	}{
		"test-case-1": {"a\x01b\x01c", 3},
		"test-case-2": {"a\x01\x01c", 3},
		"test-case-3": {"", 1},
		"test-case-4": {"abc", 1},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, PathStrIndex(tc.path))
		})
	}
}

func Test_IsChildPath(t *testing.T) {
	testCases := map[string]struct {
		parent   string
		child    string
		expected bool
	}{
		"test-case-1": {"a\x01b\x01c", "a\x01b\x01c", true},
		"test-case-2": {"a\x01b", "a\x01b\x01c", true},
		"test-case-3": {"a\x01b\x01", "a\x01b\x01c", false},
		"test-case-4": {"x\x01b\x01c", "a\x01b\x01c", false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsChildPath(tc.parent, tc.child))
		})
	}
}

func Test_newTable(t *testing.T) {
	actual := newTable(5, 6)
	require.Equal(t, 5, len(actual))
	require.Equal(t, 6, len(actual[0]))
}

func Test_TransposeTable(t *testing.T) {
	testCases := map[string]struct {
		table    [][]any
		expected [][]any
	}{
		"test-case-1": {[][]any{{1, 2, 3}}, [][]any{{1}, {2}, {3}}},
		"test-case-2": {[][]any{{1, 2, 3}, {4, 5, 6}}, [][]any{{1, 4}, {2, 5}, {3, 6}}},
		"test-case-3": {[][]any{{1}, {2}, {3}}, [][]any{{1, 2, 3}}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, TransposeTable(tc.table))
		})
	}
}

func Test_ArrowColToParquetCol(t *testing.T) {
	testCases := map[string]struct {
		field    arrow.Field
		col      arrow.Array
		expected []any
		errMsg   string
	}{
		// TODO test cases
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := ArrowColToParquetCol(tc.field, tc.col)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_nonNullableFieldContainsNullError(t *testing.T) {
	err := nonNullableFieldContainsNullError(arrow.Field{Name: "unit-test"}, 3)
	require.Equal(t, "field with name 'unit-test' is marked non-nullable but its column array contains Null value at index 3", err.Error())
}

func Test_ToPtr(t *testing.T) {
	testCases := map[string]struct {
		val any
	}{
		"bool":    {true},
		"int32":   {int32(1)},
		"int64":   {int64(1)},
		"string":  {"012345678901"},
		"float32": {float32(0.1)},
		"float64": {float64(0.1)},
		"slice":   {[]int32{1, 2, 3}},
		"map":     {map[string]int32{"a": 1, "b": 2, "c": 3}},
		"struct": {
			struct {
				id   uint64
				name string
			}{123, "abc"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ptr := ToPtr(tc.val)
			require.NotNil(t, ptr)
			require.Equal(t, tc.val, *ptr)
		})
	}
}
