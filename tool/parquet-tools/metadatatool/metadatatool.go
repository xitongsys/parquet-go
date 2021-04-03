package metadatatool

import (
	"strings"

	"github.com/xitongsys/parquet-go/reader"
)

type ColumnMeta struct {
	Name             string
	ValueCount       int64
	DistinctCount    *int64 `json:",omitempty"`
	NullCount        *int64 `json:",omitempty"`
	UncompressedSize int64
	CompressedSize   int64
}

type RowGroupMeta struct {
	Columns []ColumnMeta
}

type Metadata struct {
	RowGroups []RowGroupMeta
}

func GetMetadata(pr *reader.ParquetReader) (Metadata, error) {
	metadata := Metadata{
		RowGroups: make([]RowGroupMeta, len(pr.Footer.RowGroups)),
	}
	for i, rowGroup := range pr.Footer.RowGroups {
		for _, col := range rowGroup.Columns {
			colMeta := ColumnMeta{
				Name:             strings.Join(col.MetaData.PathInSchema, "."),
				ValueCount:       col.MetaData.NumValues,
				DistinctCount:    col.MetaData.Statistics.DistinctCount,
				NullCount:        col.MetaData.Statistics.NullCount,
				UncompressedSize: col.MetaData.TotalUncompressedSize,
				CompressedSize:   col.MetaData.TotalCompressedSize,
			}
			metadata.RowGroups[i].Columns = append(metadata.RowGroups[i].Columns, colMeta)
		}
	}
	return metadata, nil
}
