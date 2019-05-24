package sizetool

import (
	"testing"

	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/parquet"
)

func TestGetParquetFileSize(t *testing.T) {
	tests := map[string]struct {
		pr           *reader.ParquetReader
		pretty       bool
		uncompressed bool
		size         string
	}{
		"compresed_size": {
			pr: &reader.ParquetReader{
				Footer: &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						&parquet.RowGroup{
							Columns: []*parquet.ColumnChunk{
								&parquet.ColumnChunk{
									MetaData: &parquet.ColumnMetaData{
										TotalCompressedSize: 500,
									},
								},
								&parquet.ColumnChunk{
									MetaData: &parquet.ColumnMetaData{
										TotalCompressedSize: 501,
									},
								},
							},
						},
						&parquet.RowGroup{
							Columns: []*parquet.ColumnChunk{
								&parquet.ColumnChunk{
									MetaData: &parquet.ColumnMetaData{
										TotalCompressedSize: 233,
									},
								},
							},
						},
					},
				},
			},
			pretty:       false,
			uncompressed: false,
			size:         "foo: 1234 bytes",
		},
		"uncompresed_size": {
			pr: &reader.ParquetReader{
				Footer: &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						&parquet.RowGroup{
							TotalByteSize: 1001,
						},
						&parquet.RowGroup{
							TotalByteSize: 24,
						},
					},
				},
			},
			pretty:       false,
			uncompressed: true,
			size:         "foo: 1025 bytes",
		},
		"pretty_size_KB": {
			pr: &reader.ParquetReader{
				Footer: &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						&parquet.RowGroup{
							TotalByteSize: 1001,
						},
						&parquet.RowGroup{
							TotalByteSize: 999,
						},
					},
				},
			},
			pretty:       true,
			uncompressed: true,
			size:         "foo: 1.953 KB",
		},
	}
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			size := GetParquetFileSize("foo", test.pr, test.pretty, test.uncompressed)
			if size != test.size {
				t.Errorf("expected: %s, got: %s", test.size, size)
			}
		})
	}
}
