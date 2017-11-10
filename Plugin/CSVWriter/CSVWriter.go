package CSVWriter

import (
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	. "github.com/xitongsys/parquet-go/Layout"
	. "github.com/xitongsys/parquet-go/ParquetHandler"
	. "github.com/xitongsys/parquet-go/ParquetType"
	. "github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"strings"
)

//Write handler for CSV data
type CSVWriterHandler struct {
	SchemaHandler *SchemaHandler
	NP            int64
	Footer        *parquet.FileMetaData
	RowGroups     []*RowGroup

	PFile ParquetFile

	PageSize     int64
	RowGroupSize int64
	Offset       int64
	Record       [][]interface{}
	Metadata     []MetadataType
	RecAveSize   int64
	Size         int64
}

//Create a CSV writer handler
func NewCSVWriterHandler() *CSVWriterHandler {
	res := new(CSVWriterHandler)
	res.NP = 1
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	return res
}

//Convert the column names to lowercase
func (self *CSVWriterHandler) NameToLower() {
	for _, schema := range self.Footer.Schema {
		schema.Name = strings.ToLower(schema.Name)
	}
	for _, rowGroup := range self.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			ln := len(chunk.MetaData.PathInSchema)
			for i := 0; i < ln; i++ {
				chunk.MetaData.PathInSchema[i] = strings.ToLower(chunk.MetaData.PathInSchema[i])
			}
		}
	}
}

//Write init function for CSV writer
func (self *CSVWriterHandler) WriteInit(md []MetadataType, pfile ParquetFile, np int64, recordAveSize int64) {
	self.SchemaHandler = NewSchemaHandlerFromMetadata(md)
	self.Metadata = md
	self.PFile = pfile
	self.NP = np
	self.RecAveSize = recordAveSize
	self.Footer = parquet.NewFileMetaData()
	self.Footer.Version = 1
	self.Footer.Schema = append(self.Footer.Schema, self.SchemaHandler.SchemaElements...)
	self.Offset = 4
	self.PFile.Write([]byte("PAR1"))
}

//Write string values to parquet file
func (self *CSVWriterHandler) WriteString(recs []*string) {
	self.Size += self.RecAveSize

	ln := len(recs)
	rec := make([]interface{}, ln)
	for i := 0; i < ln; i++ {
		rec[i] = nil
		if recs[i] != nil {
			rec[i] = StrToParquetType(*recs[i], self.Metadata[i].Type)
		}
	}
	self.Record = append(self.Record, rec)

	if self.Size > self.RowGroupSize {
		self.Flush()
	}
}

//Write parquet values to parquet file
func (self *CSVWriterHandler) Write(rec []interface{}) {
	self.Size += self.RecAveSize
	self.Record = append(self.Record, rec)

	if self.Size > self.RowGroupSize {
		self.Flush()
	}
}

//Write footer to parquet file and stop writing
func (self *CSVWriterHandler) WriteStop() {
	//self.Flush()
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	footerBuf, _ := ts.Write(self.Footer)

	self.PFile.Write(footerBuf)
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))
	self.PFile.Write(footerSizeBuf)
	self.PFile.Write([]byte("PAR1"))
}

//Flush the write buffer to parquet file
func (self *CSVWriterHandler) Flush() {
	pagesMapList := make([]map[string][]*Page, self.NP)
	for i := 0; i < int(self.NP); i++ {
		pagesMapList[i] = make(map[string][]*Page)
	}

	doneChan := make(chan int)
	l := int64(len(self.Record))
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

			tableMap := MarshalCSV(self.Record, b, e, self.Metadata, self.SchemaHandler)
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

	for k := 0; k < len(self.SchemaHandler.SchemaElements); k++ {
		//for _, chunk := range chunkMap {
		schema := self.SchemaHandler.SchemaElements[k]
		if schema.GetNumChildren() > 0 {
			continue
		}
		chunk := chunkMap[self.SchemaHandler.IndexMap[int32(k)]]
		rowGroup.Chunks = append(rowGroup.Chunks, chunk)
		rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalCompressedSize
		rowGroup.RowGroupHeader.Columns = append(rowGroup.RowGroupHeader.Columns, chunk.ChunkHeader)
	}
	rowGroup.RowGroupHeader.NumRows = int64(len(self.Record))

	for k := 0; k < len(rowGroup.Chunks); k++ {
		rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = self.Offset
		rowGroup.Chunks[k].ChunkHeader.FileOffset = self.Offset

		for l := 0; l < len(rowGroup.Chunks[k].Pages); l++ {
			data := rowGroup.Chunks[k].Pages[l].RawData
			self.PFile.Write(data)
			self.Offset += int64(len(data))
		}
	}
	self.Footer.NumRows += int64(len(self.Record))
	self.Footer.RowGroups = append(self.Footer.RowGroups, rowGroup.RowGroupHeader)
	self.Size = 0
	self.Record = self.Record[0:0]
}
