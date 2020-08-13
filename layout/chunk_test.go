package layout

import (
	"testing"

	"github.com/syucream/parquet-go/parquet"
)

func TestPagesToChunk(t *testing.T) {
	pages := []*Page{
		{
			Header: &parquet.PageHeader{
				DataPageHeader: &parquet.DataPageHeader{
					NumValues: 3,
				},
				UncompressedPageSize: 10,
				CompressedPageSize:   10,
			},
			RawData:      []byte("hogehoge"),
			CompressType: parquet.CompressionCodec_GZIP,
			Path:         []string{},
			Schema: &parquet.SchemaElement{
				Type:          parquet.TypePtr(parquet.Type_INT64),
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64),
			},
			MaxVal: int64(100),
			MinVal: int64(-100),
		},
	}

	func() {
		defer func() {
			if err := recover(); err != nil {
				t.Error(err)
			}
		}()
		_ = PagesToChunk(pages)
	}()
}

func TestPagesToDictChunk(t *testing.T) {
	pages := []*Page{
		{
			Header: &parquet.PageHeader{
				DataPageHeader: &parquet.DataPageHeader{
					NumValues: 3,
				},
				UncompressedPageSize: 10,
				CompressedPageSize:   10,
			},
			RawData:      []byte("hogehoge"),
			CompressType: parquet.CompressionCodec_GZIP,
			Path:         []string{},
			Schema: &parquet.SchemaElement{
				Type:          parquet.TypePtr(parquet.Type_INT64),
				ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64),
			},
			MaxVal: int64(100),
			MinVal: int64(-100),
		},
	}

	func() {
		defer func() {
			if err := recover(); err != nil {
				t.Error(err)
			}
		}()
		_ = PagesToDictChunk(pages)
	}()
}
