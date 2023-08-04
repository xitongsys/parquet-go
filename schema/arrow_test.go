package schema

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/stretchr/testify/assert"
)

func TestTypeConversion(t *testing.T) {
	tests := []struct {
		title                   string
		testSchema              *arrow.Schema
		expectedParquetMetaData []string
		expectedErr             bool
	}{
		{
			title: "test primitive type conversion",
			testSchema: arrow.NewSchema([]arrow.Field{
				{Name: "f1-i8", Type: arrow.PrimitiveTypes.Int8},
				{Name: "f1-i16", Type: arrow.PrimitiveTypes.Int16},
				{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
				{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
				{Name: "f1-ui8", Type: arrow.PrimitiveTypes.Uint8},
				{Name: "f1-ui16", Type: arrow.PrimitiveTypes.Uint16},
				{Name: "f1-ui32", Type: arrow.PrimitiveTypes.Uint32},
				{Name: "f1-ui64", Type: arrow.PrimitiveTypes.Uint64},
				{Name: "f1-f32", Type: arrow.PrimitiveTypes.Float32},
				{Name: "f1-f62", Type: arrow.PrimitiveTypes.Float64},
				{Name: "f1-d32", Type: arrow.PrimitiveTypes.Date32},
				{Name: "f1-d64", Type: arrow.PrimitiveTypes.Date64},
				{Name: "null-i8", Type: arrow.PrimitiveTypes.Int8,
					Nullable: true},
				{Name: "null-i16", Type: arrow.PrimitiveTypes.Int16,
					Nullable: true},
				{Name: "null-i32", Type: arrow.PrimitiveTypes.Int32,
					Nullable: true},
				{Name: "null-i64", Type: arrow.PrimitiveTypes.Int64,
					Nullable: true},
				{Name: "null-ui8", Type: arrow.PrimitiveTypes.Uint8,
					Nullable: true},
				{Name: "null-ui16", Type: arrow.PrimitiveTypes.Uint16,
					Nullable: true},
				{Name: "null-ui32", Type: arrow.PrimitiveTypes.Uint32,
					Nullable: true},
				{Name: "null-ui64", Type: arrow.PrimitiveTypes.Uint64,
					Nullable: true},
				{Name: "null-f32", Type: arrow.PrimitiveTypes.Float32,
					Nullable: true},
				{Name: "null-f62", Type: arrow.PrimitiveTypes.Float64,
					Nullable: true},
				{Name: "null-d32", Type: arrow.PrimitiveTypes.Date32,
					Nullable: true},
				{Name: "null-d64", Type: arrow.PrimitiveTypes.Date64,
					Nullable: true},
			}, nil),
			expectedParquetMetaData: []string{
				"name=f1-i8, type=INT32, convertedtype=INT_8, " +
					"repetitiontype=REQUIRED",
				"name=f1-i16, type=INT32, convertedtype=INT_16, " +
					"repetitiontype=REQUIRED",
				"name=f1-i32, type=INT32, repetitiontype=REQUIRED",
				"name=f1-i64, type=INT64, repetitiontype=REQUIRED",
				"name=f1-ui8, type=INT32, convertedtype=UINT_8, " +
					"repetitiontype=REQUIRED",
				"name=f1-ui16, type=INT32, convertedtype=UINT_16, " +
					"repetitiontype=REQUIRED",
				"name=f1-ui32, type=INT32, convertedtype=UINT_32, " +
					"repetitiontype=REQUIRED",
				"name=f1-ui64, type=INT64, convertedtype=UINT_64, " +
					"repetitiontype=REQUIRED",
				"name=f1-f32, type=FLOAT, repetitiontype=REQUIRED",
				"name=f1-f62, type=DOUBLE, repetitiontype=REQUIRED",
				"name=f1-d32, type=INT32, convertedtype=DATE, " +
					"repetitiontype=REQUIRED",
				"name=f1-d64, type=INT32, convertedtype=DATE, " +
					"repetitiontype=REQUIRED",
				"name=null-i8, type=INT32, convertedtype=INT_8, " +
					"repetitiontype=OPTIONAL",
				"name=null-i16, type=INT32, convertedtype=INT_16, " +
					"repetitiontype=OPTIONAL",
				"name=null-i32, type=INT32, repetitiontype=OPTIONAL",
				"name=null-i64, type=INT64, repetitiontype=OPTIONAL",
				"name=null-ui8, type=INT32, convertedtype=UINT_8, " +
					"repetitiontype=OPTIONAL",
				"name=null-ui16, type=INT32, convertedtype=UINT_16, " +
					"repetitiontype=OPTIONAL",
				"name=null-ui32, type=INT32, convertedtype=UINT_32, " +
					"repetitiontype=OPTIONAL",
				"name=null-ui64, type=INT64, convertedtype=UINT_64, " +
					"repetitiontype=OPTIONAL",
				"name=null-f32, type=FLOAT, repetitiontype=OPTIONAL",
				"name=null-f62, type=DOUBLE, repetitiontype=OPTIONAL",
				"name=null-d32, type=INT32, convertedtype=DATE, " +
					"repetitiontype=OPTIONAL",
				"name=null-d64, type=INT32, convertedtype=DATE, " +
					"repetitiontype=OPTIONAL",
			},
			expectedErr: false,
		},
		{
			title: "test binary type conversion",
			testSchema: arrow.NewSchema([]arrow.Field{
				{Name: "f1-string", Type: arrow.BinaryTypes.String},
				{Name: "f1-binary", Type: arrow.BinaryTypes.Binary},
				{Name: "null-string", Type: arrow.BinaryTypes.String,
					Nullable: true},
				{Name: "null-binary", Type: arrow.BinaryTypes.Binary,
					Nullable: true},
			}, nil),
			expectedParquetMetaData: []string{
				"name=f1-string, type=BYTE_ARRAY, convertedtype=UTF8, " +
					"repetitiontype=REQUIRED",
				"name=f1-binary, type=BYTE_ARRAY, repetitiontype=REQUIRED",
				"name=null-string, type=BYTE_ARRAY, convertedtype=UTF8, " +
					"repetitiontype=OPTIONAL",
				"name=null-binary, type=BYTE_ARRAY, repetitiontype=OPTIONAL",
			},
			expectedErr: false,
		},
		{
			title: "test fixed width type conversion",
			testSchema: arrow.NewSchema([]arrow.Field{
				{Name: "f1-bool", Type: arrow.FixedWidthTypes.Boolean},
				{Name: "f1-d32", Type: arrow.FixedWidthTypes.Date32},
				{Name: "f1-d64", Type: arrow.FixedWidthTypes.Date64},
				{Name: "f1-t32ms", Type: arrow.FixedWidthTypes.Time32ms},
				{Name: "f1-tsms", Type: arrow.FixedWidthTypes.Timestamp_ms},
				{Name: "null-bool", Type: arrow.FixedWidthTypes.Boolean,
					Nullable: true},
				{Name: "null-d32", Type: arrow.FixedWidthTypes.Date32,
					Nullable: true},
				{Name: "null-d64", Type: arrow.FixedWidthTypes.Date64,
					Nullable: true},
				{Name: "null-t32ms", Type: arrow.FixedWidthTypes.Time32ms,
					Nullable: true},
				{Name: "null-tsms", Type: arrow.FixedWidthTypes.Timestamp_ms,
					Nullable: true},
			}, nil),
			expectedParquetMetaData: []string{
				"name=f1-bool, type=BOOLEAN, repetitiontype=REQUIRED",
				"name=f1-d32, type=INT32, convertedtype=DATE, " +
					"repetitiontype=REQUIRED",
				"name=f1-d64, type=INT32, convertedtype=DATE, " +
					"repetitiontype=REQUIRED",
				"name=f1-t32ms, type=INT32, convertedtype=TIME_MILLIS, " +
					"repetitiontype=REQUIRED",
				"name=f1-tsms, type=INT64, convertedtype=TIMESTAMP_MILLIS, " +
					"repetitiontype=REQUIRED",
				"name=null-bool, type=BOOLEAN, repetitiontype=OPTIONAL",
				"name=null-d32, type=INT32, convertedtype=DATE, " +
					"repetitiontype=OPTIONAL",
				"name=null-d64, type=INT32, convertedtype=DATE, " +
					"repetitiontype=OPTIONAL",
				"name=null-t32ms, type=INT32, convertedtype=TIME_MILLIS, " +
					"repetitiontype=OPTIONAL",
				"name=null-tsms, type=INT64, convertedtype=TIMESTAMP_MILLIS, " +
					"repetitiontype=OPTIONAL",
			},
			expectedErr: false,
		},
		{
			title: "test non supported types",
			testSchema: arrow.NewSchema([]arrow.Field{
				{Name: "f1-t64us", Type: arrow.FixedWidthTypes.Time64us},
				{Name: "f1-t32s", Type: arrow.FixedWidthTypes.Time32s},
				{Name: "f1-tsns", Type: arrow.FixedWidthTypes.Timestamp_ns},
				{Name: "f1-tss", Type: arrow.FixedWidthTypes.Timestamp_s},
				{Name: "null-t64us", Type: arrow.FixedWidthTypes.Time64us,
					Nullable: true},
				{Name: "null-t32s", Type: arrow.FixedWidthTypes.Time32s,
					Nullable: true},
				{Name: "null-tsns", Type: arrow.FixedWidthTypes.Timestamp_ns,
					Nullable: true},
				{Name: "null-tss", Type: arrow.FixedWidthTypes.Timestamp_s,
					Nullable: true},
			}, nil),
			expectedParquetMetaData: []string{},
			expectedErr:             true,
		},
	}
	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			actualMetaData, err :=
				ConvertArrowToParquetSchema(test.testSchema)
			if err != nil {
				assert.True(t, test.expectedErr)
			} else {
				assert.False(t, test.expectedErr)
			}
			for k, v := range test.expectedParquetMetaData {
				assert.Equal(t, v, actualMetaData[k])
			}
		})
	}
}
