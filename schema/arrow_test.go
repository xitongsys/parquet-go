package schema

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
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
			}, nil),
			expectedParquetMetaData: []string{
				"name=f1-i8, type=INT32, convertedtype=INT_8",
				"name=f1-i16, type=INT32, convertedtype=INT_16",
				"name=f1-i32, type=INT32, convertedtype=INT_32",
				"name=f1-i64, type=INT64, convertedtype=INT_64",
				"name=f1-ui8, type=INT32, convertedtype=UINT_8",
				"name=f1-ui16, type=INT32, convertedtype=UINT_16",
				"name=f1-ui32, type=INT32, convertedtype=UINT_32",
				"name=f1-ui64, type=INT64, convertedtype=UINT_64",
				"name=f1-f32, type=FLOAT",
				"name=f1-f62, type=DOUBLE",
				"name=f1-d32, type=INT32, convertedtype=DATE",
				"name=f1-d64, type=INT32, convertedtype=DATE",
			},
			expectedErr: false,
		},
		{
			title: "test binary type conversion",
			testSchema: arrow.NewSchema([]arrow.Field{
				{Name: "f1-string", Type: arrow.BinaryTypes.String},
				{Name: "f1-binary", Type: arrow.BinaryTypes.Binary},
			}, nil),
			expectedParquetMetaData: []string{
				"name=f1-string, type=BYTE_ARRAY, convertedtype=UTF8",
				"name=f1-binary, type=BYTE_ARRAY",
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
			}, nil),
			expectedParquetMetaData: []string{
				"name=f1-bool, type=BOOLEAN",
				"name=f1-d32, type=INT32, convertedtype=DATE",
				"name=f1-d64, type=INT32, convertedtype=DATE",
				"name=f1-t32ms, type=INT32, convertedtype=TIME_MILLIS",
				"name=f1-tsms, type=INT64, convertedtype=TIMESTAMP_MILLIS",
			},
			expectedErr: false,
		},
		{
			title: "test non supported types",
			testSchema: arrow.NewSchema([]arrow.Field{
				{Name: "f1-bool", Type: arrow.FixedWidthTypes.Time64us},
				{Name: "f1-d64", Type: arrow.FixedWidthTypes.Time64us},
				{Name: "f1-d64", Type: arrow.FixedWidthTypes.Time32s},
				{Name: "f1-d32", Type: arrow.FixedWidthTypes.Timestamp_ns},
				{Name: "f1-d64", Type: arrow.FixedWidthTypes.Timestamp_s},
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
