package ParquetHandler

import (
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/parquet"
)

//Read one RowGroup from parquet file
func (self *ParquetHandler) ReadRowGroup(rowGroupHeader *parquet.RowGroup) *Layout.RowGroup {
	rowGroup := new(Layout.RowGroup)
	rowGroup.RowGroupHeader = rowGroupHeader

	columnChunks := rowGroupHeader.GetColumns()
	ln := int64(len(columnChunks))
	chunksList := make([][]*Layout.Chunk, self.NP)
	for i := int64(0); i < self.NP; i++ {
		chunksList[i] = make([]*Layout.Chunk, 0)
	}

	delta := (ln + self.NP - 1) / self.NP
	doneChan := make(chan int, 1)

	for c := int64(0); c < self.NP; c++ {
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
				PFile := self.PFile
				if columnChunks[i].FilePath != nil {
					PFile, _ = self.PFile.Open(*columnChunks[i].FilePath)
				} else {
					PFile, _ = self.PFile.Open("")
				}
				size := columnChunks[i].MetaData.GetTotalCompressedSize()
				thriftReader := ConvertToThriftReader(PFile, offset, size)
				chunk := self.ReadChunk(thriftReader, self.SchemaHandler, columnChunks[i])
				chunksList[index] = append(chunksList[index], chunk)
				PFile.Close()
			}
			doneChan <- 1
		}(c, bgn, end)
	}

	for c := int64(0); c < self.NP; c++ {
		<-doneChan
	}

	for c := int64(0); c < self.NP; c++ {
		if len(chunksList[c]) <= 0 {
			continue
		}
		rowGroup.Chunks = append(rowGroup.Chunks, chunksList[c]...)
	}

	return rowGroup
}
