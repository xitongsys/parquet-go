package ParquetHandler

import (
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	. "github.com/xitongsys/parquet-go/Layout"
	. "github.com/xitongsys/parquet-go/Marshal"
	. "github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

func (self *ParquetHandler) WriteInit(pfile ParquetFile, obj interface{}, np int64, objAveSize int64) {
	self.SchemaHandler = NewSchemaHandlerFromStruct(obj)
	//log.Println(self.SchemaHandler)
	self.NP = np
	self.ObjAveSize = objAveSize
	self.PFile = pfile
	self.Footer = parquet.NewFileMetaData()
	self.Footer.Version = 1
	self.Footer.Schema = append(self.Footer.Schema, self.SchemaHandler.SchemaElements...)
}

func (self *ParquetHandler) WriteStop() {
	self.Flush()

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	footerBuf, _ := ts.Write(self.Footer)

	self.PFile.Write(footerBuf)
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))
	self.PFile.Write(footerSizeBuf)
	self.PFile.Write([]byte("PAR1"))
	//log.Println(self.Footer)
}

func (self *ParquetHandler) Write(src interface{}) {
	//self.Size += SizeOf(reflect.ValueOf(src))
	self.Size += self.ObjAveSize
	self.Objs = append(self.Objs, src)

	if self.Size >= self.RowGroupSize {
		self.Flush()
	}
}

func (self *ParquetHandler) Flush() {
	pagesMapList := make([]map[string][]*Page, self.NP)
	for i := 0; i < int(self.NP); i++ {
		pagesMapList[i] = make(map[string][]*Page)
	}

	doneChan := make(chan int)
	l := int64(len(self.Objs))
	var c int64 = 0
	delta := (l + self.NP - 1) / self.NP
	for c = 0; c < self.NP; c++ {
		bgn := c * delta
		end := bgn + delta
		if end > l {
			end = l
		}
		if bgn >= l {
			bgn, end = l, l
		}

		go func(b, e int, index int64) {
			if e <= b {
				doneChan <- 0
				return
			}

			tableMap := Marshal(self.Objs, b, e, self.SchemaHandler)
			for name, table := range *tableMap {
				pagesMapList[index][name], _ = TableToDataPages(table, int32(self.PageSize),
					parquet.CompressionCodec_SNAPPY)
			}

			doneChan <- 0
		}(int(bgn), int(end), c)
	}

	for c = 0; c < self.NP; c++ {
		<-doneChan
	}

	totalPagesMap := make(map[string][]*Page)
	for _, pagesMap := range pagesMapList {
		for name, pages := range pagesMap {
			if _, ok := totalPagesMap[name]; !ok {
				totalPagesMap[name] = pages
			} else {
				totalPagesMap[name] = append(totalPagesMap[name], pages...)
			}
		}
	}

	//pages -> chunk
	chunkMap := make(map[string]*Chunk)
	for name, pages := range totalPagesMap {
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
	rowGroup.RowGroupHeader.NumRows = int64(len(self.Objs))

	for k := 0; k < len(rowGroup.Chunks); k++ {
		rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = self.Offset
		rowGroup.Chunks[k].ChunkHeader.FileOffset = self.Offset

		for l := 0; l < len(rowGroup.Chunks[k].Pages); l++ {
			data := rowGroup.Chunks[k].Pages[l].RawData
			self.PFile.Write(data)
			self.Offset += int64(len(data))
		}
	}
	self.Footer.NumRows += int64(len(self.Objs))
	self.Footer.RowGroups = append(self.Footer.RowGroups, rowGroup.RowGroupHeader)
	self.Size = 0
	self.Objs = self.Objs[0:0]
}
