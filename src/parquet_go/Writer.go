package parquet_go

import (
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"os"
	"parquet"
	"reflect"
)

func WriteTo(file *os.File, srcInterface interface{}, schemaHandler *SchemaHandler){
	var pageSize int64 = 8*1024 //8K
	var rowGroupSize int64 = 256*1024*1024 //256MB
	
	src := reflect.ValueOf(srcInterface)
	ln := src.Len()

	//create rowGroups
	rowGroups := make([]*RowGroup, 0)
	i:=0
	for i < ln {
		j:=i
		var size int64 = 0
		for j<ln && size < rowGroupSize {
			size += SizeOf(src.Index(j))
			j++
		}
		tableMap := Marshal(srcInterface, i, j, schemaHandler)

		//table -> pages
		pagesMap := make(map[string][]*Page)
		for name, table := range *tableMap {
			//log.Println(name,table)
			pagesMap[name], _ = TableToPages(table, int32(pageSize), parquet.CompressionCodec_SNAPPY)
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
		rowGroups = append(rowGroups, rowGroup)

		i = j
	}

	footer := parquet.NewFileMetaData()
	footer.Version = 1
	footer.Schema = append(footer.Schema, schemaHandler.SchemaList...)
	
	file.Write([]byte("PAR1"))
	var offset int64 = 4

	for i:=0; i<len(rowGroups);i++ {
		footer.RowGroups = append(footer.RowGroups, rowGroups[i].RowGroupHeader)
		footer.NumRows += rowGroups[i].RowGroupHeader.NumRows
		for j:= 0; j<len(rowGroups[i].Chunks); j++ {
			rowGroups[i].Chunks[j].ChunkHeader.MetaData.DataPageOffset = offset 
			rowGroups[i].Chunks[j].ChunkHeader.FileOffset = offset
			
			for k:=0; k<len(rowGroups[i].Chunks[j].Pages); k++ {
				data := rowGroups[i].Chunks[j].Pages[k].RawData
				file.Write(data)
				offset += int64(len(data))
			}

		}
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	footerBuf, _ := ts.Write(footer)

	file.Write(footerBuf)
	footerSizeBuf := make([]byte,4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))
	file.Write(footerSizeBuf)
	file.Write([]byte("PAR1"))
	
}
