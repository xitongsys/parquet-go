package Layout

import (
	"github.com/pmalekn/parquet-go/Common"
	"github.com/pmalekn/parquet-go/ParquetFile"
	"github.com/pmalekn/parquet-go/SchemaHandler"
	"github.com/pmalekn/parquet-go/parquet"
)

//RowGroup stores the RowGroup in parquet file
type RowGroup struct {
	Chunks         []*Chunk
	RowGroupHeader *parquet.RowGroup
}

//Create a RowGroup
func NewRowGroup() *RowGroup {
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = parquet.NewRowGroup()
	return rowGroup
}

//Convert a RowGroup to table map
func (rowGroup *RowGroup) RowGroupToTableMap() *map[string]*Table {
	tableMap := make(map[string]*Table, 0)
	for _, chunk := range rowGroup.Chunks {
		pathStr := ""
		for _, page := range chunk.Pages {
			if pathStr == "" {
				pathStr = Common.PathToStr(page.DataTable.Path)
			}
			if _, ok := tableMap[pathStr]; !ok {
				tableMap[pathStr] = NewTableFromTable(page.DataTable)
			}
			tableMap[pathStr].Merge(page.DataTable)
		}
	}
	return &tableMap
}

//Read one RowGroup from parquet file (Deprecated)
func ReadRowGroup(rowGroupHeader *parquet.RowGroup, PFile ParquetFile.ParquetFile, schemaHandler *SchemaHandler.SchemaHandler, NP int64) (*RowGroup, error) {
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = rowGroupHeader

	columnChunks := rowGroupHeader.GetColumns()
	ln := int64(len(columnChunks))
	chunksList := make([][]*Chunk, NP)
	for i := int64(0); i < NP; i++ {
		chunksList[i] = make([]*Chunk, 0)
	}

	delta := (ln + NP - 1) / NP
	doneChan := make(chan int, 1)

	for c := int64(0); c < NP; c++ {
		bgn := c * delta
		end := bgn + delta
		if end > ln {
			end = ln
		}
		if bgn >= ln {
			bgn, end = ln, ln
		}

		go func(index int64, bgn int64, end int64) {
			for i := bgn; i < end; i++ {
				offset := columnChunks[i].FileOffset
				PFile := PFile
				if columnChunks[i].FilePath != nil {
					PFile, _ = PFile.Open(*columnChunks[i].FilePath)
				} else {
					PFile, _ = PFile.Open("")
				}
				size := columnChunks[i].MetaData.GetTotalCompressedSize()
				thriftReader := ParquetFile.ConvertToThriftReader(PFile, offset, size)
				chunk, _ := ReadChunk(thriftReader, schemaHandler, columnChunks[i])
				chunksList[index] = append(chunksList[index], chunk)
				PFile.Close()
			}
			doneChan <- 1
		}(c, bgn, end)
	}

	for c := int64(0); c < NP; c++ {
		<-doneChan
	}

	for c := int64(0); c < NP; c++ {
		if len(chunksList[c]) <= 0 {
			continue
		}
		rowGroup.Chunks = append(rowGroup.Chunks, chunksList[c]...)
	}

	return rowGroup, nil
}
