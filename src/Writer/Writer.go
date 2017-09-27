package Writer

import (
	. "Common"
	. "Layout"
	. "Marshal"
	. "SchemaHandler"
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"os"
	"parquet"
	"reflect"
)

func WriteParquet(file *os.File, srcInterface interface{}, schemaHandler *SchemaHandler, np int) {
	var pageSize int64 = 8 * 1024              //8K
	var rowGroupSize int64 = 256 * 1024 * 1024 //256MB

	src := reflect.ValueOf(srcInterface)
	ln := src.Len()

	footer := parquet.NewFileMetaData()
	footer.Version = 1
	footer.Schema = append(footer.Schema, schemaHandler.SchemaElements...)

	file.Write([]byte("PAR1"))
	var offset int64 = 4

	i := 0
	for i < ln {
		j := i
		var size int64 = 0
		for j < ln && size < rowGroupSize {
			size += SizeOf(src.Index(j))
			j++
		}

		tableMapList := make([]*map[string]*Table, np)
		doneChan := make(chan int)
		delta := (j - i) / np
		for c := 0; c < np; c++ {
			bgn := i + c*delta
			end := bgn + delta
			if c == np-1 {
				end = j
			}
			go func(index int) {
				tableMapList[index] = Marshal(srcInterface, bgn, end, schemaHandler)
				doneChan <- 0
			}(c)
		}

		for c := 0; c < np; c++ {
			<-doneChan
		}

		//table -> pages
		pagesMap := make(map[string][]*Page)
		for _, tableMap := range tableMapList {
			for name := range *tableMap {
				pagesMap[name] = make([]*Page, 0)
			}
		}
		nameList := make([]string, len(pagesMap))
		k := 0
		for name := range pagesMap {
			nameList[k] = name
			k++
		}

		delta = (len(nameList)) / np
		for c := 0; c < np; c++ {
			bgn := c * delta
			end := bgn + delta
			if c == np-1 {
				end = len(nameList)
			}

			go func(names []string) {
				for _, name := range names {
					for _, tableMap := range tableMapList {
						tmp, _ := TableToDataPages((*tableMap)[name], int32(pageSize), parquet.CompressionCodec_SNAPPY)
						pagesMap[name] = append(pagesMap[name], tmp...)
					}
				}
				doneChan <- 0
			}(nameList[bgn:end])
		}

		for c := 0; c < np; c++ {
			<-doneChan
		}

		//pages -> chunk
		chunkMap := make(map[string]*Chunk)
		for name, pages := range pagesMap {
			chunkMap[name] = PagesToChunk(pages)
		}

		//chunks -> rowGroup
		rowGroup := NewRowGroup()
		rowGroup.RowGroupHeader.Columns = make([]*parquet.ColumnChunk, 0)
		for _, chunk := range chunkMap {
			rowGroup.Chunks = append(rowGroup.Chunks, chunk)
			rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalCompressedSize
			rowGroup.RowGroupHeader.Columns = append(rowGroup.RowGroupHeader.Columns, chunk.ChunkHeader)
		}
		rowGroup.RowGroupHeader.NumRows = int64(j - i)

		for k := 0; k < len(rowGroup.Chunks); k++ {
			rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = offset
			rowGroup.Chunks[k].ChunkHeader.FileOffset = offset

			for l := 0; l < len(rowGroup.Chunks[k].Pages); l++ {
				data := rowGroup.Chunks[k].Pages[l].RawData
				file.Write(data)
				offset += int64(len(data))
			}
		}
		footer.NumRows += int64(j - i)
		footer.RowGroups = append(footer.RowGroups, rowGroup.RowGroupHeader)

		i = j
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	footerBuf, _ := ts.Write(footer)

	file.Write(footerBuf)
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))
	file.Write(footerSizeBuf)
	file.Write([]byte("PAR1"))

	log.Println(footer)

}
