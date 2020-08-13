package layout

import (
	"testing"

	"github.com/syucream/parquet-go/parquet"
)

func TestPage_EncodingValues(t *testing.T) {
	cases := []struct {
		tpe *parquet.Type
		enc parquet.Encoding
		v   []interface{}
	}{
		// Plain
		{
			tpe: parquet.TypePtr(parquet.Type_INT64),
			enc: parquet.Encoding_PLAIN,
			v:   []interface{}{int64(42)},
		},

		// RLE
		{
			tpe: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			enc: parquet.Encoding_RLE,
			v:   []interface{}{string("test")},
		},

		// Delta
		{
			tpe: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			enc: parquet.Encoding_DELTA_BINARY_PACKED,
			v:   []interface{}{[]byte("test")},
		},

		// Delta Byte Array
		{
			tpe: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			enc: parquet.Encoding_DELTA_BYTE_ARRAY,
			v:   []interface{}{[]byte("test")},
		},

		// Delta Length Byte Array
		{
			tpe: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			enc: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			v:   []interface{}{[]byte("test")},
		},
	}

	for _, c := range cases {
		page := NewDataPage()
		page.Schema = &parquet.SchemaElement{
			Type: c.tpe,
		}
		page.encoding = c.enc
		page.bitWidths = int32(len(c.v))

		func() {
			defer func() {
				if err := recover(); err != nil {
					t.Error(err)
				}
			}()
			_ = page.EncodingValues(c.v)
		}()
	}
}
