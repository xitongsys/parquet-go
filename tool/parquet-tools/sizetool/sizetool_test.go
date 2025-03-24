package sizetool

import (
	"testing"

	"github.com/hangxie/parquet-go/parquet"
	"github.com/hangxie/parquet-go/reader"
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
						{
							Columns: []*parquet.ColumnChunk{
								{
									MetaData: &parquet.ColumnMetaData{
										TotalCompressedSize: 500,
									},
								},
								{
									MetaData: &parquet.ColumnMetaData{
										TotalCompressedSize: 501,
									},
								},
							},
						},
						{
							Columns: []*parquet.ColumnChunk{
								{
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
						{
							TotalByteSize: 1001,
						},
						{
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
						{
							TotalByteSize: 1001,
						},
						{
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
