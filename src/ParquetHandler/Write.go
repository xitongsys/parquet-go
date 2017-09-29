package ParquetHandler

import (
	. "Common"
	. "Layout"
	. "Marshal"
	. "SchemaHandler"
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"parquet"
	"reflect"
	"sync"
)

func (self *ParquetHandler) WriteInit(pfile ParquetFile, obj interface{}, np int64) {
	self.SchemaHandler = NewSchemaHandlerFromStruct(obj)
	log.Println(self.SchemaHandler)
	self.NP = np
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
	log.Println(self.Footer)
}

func (self *ParquetHandler) Write(src interface{}) {
	self.Size += SizeOf(reflect.ValueOf(src))
	self.Objs = append(self.Objs, src)

	if self.Size >= self.RowGroupSize {
		self.Flush()
	}
}

func (self *ParquetHandler) Flush() {
	tableMapList := make([]*map[string]*Table, self.NP)
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

		go func(index int64) {
			tableMapList[index] = Marshal(self.Objs, int(bgn), int(end), self.SchemaHandler)
			doneChan <- 0
		}(c)
	}

	for c = 0; c < self.NP; c++ {
		<-doneChan
	}

	//table->pages
	var mutex = &sync.Mutex{}
	pagesMap := make(map[string][]*Page)
	for _, tableMap := range tableMapList {
		if tableMap == nil {
			continue
		}
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

	l = int64(len(nameList))
	delta = (l + self.NP - 1) / self.NP
	for c = 0; c < self.NP; c++ {
		bgn := c * delta
		end := bgn + delta
		if end > l {
			end = l
		}
		if bgn >= l {
			bgn, end = 0, 0
		}

		go func(names []string) {
			for _, name := range names {
				for _, tableMap := range tableMapList {
					tmp, _ := TableToDataPages((*tableMap)[name], int32(self.PageSize),
						parquet.CompressionCodec_SNAPPY)

					mutex.Lock()
					pagesMap[name] = append(pagesMap[name], tmp...)
					mutex.Unlock()
				}
			}
			doneChan <- 0
		}(nameList[bgn:end])
	}
	for c = 0; c < self.NP; c++ {
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
