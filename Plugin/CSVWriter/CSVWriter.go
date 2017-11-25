package CSVWriter

import (
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
	"strings"
)

//Write handler for CSV data
type CSVWriter struct {
	SchemaHandler *SchemaHandler.SchemaHandler
	NP            int64
	Footer        *parquet.FileMetaData
	RowGroups     []*Layout.RowGroup

	PFile ParquetFile.ParquetFile

	PageSize     int64
	RowGroupSize int64
	Offset       int64

	Objs              [][]interface{}
	ObjsSize          int64
	ObjSize           int64
	CheckSizeCritical int64
	Metadata          []MetadataType

	PagesMapBuf map[string][]*Layout.Page
	Size        int64
	NumRows     int64
}

//Create CSV writer
func NewCSVWriter(md []MetadataType, pfile ParquetFile.ParquetFile, np int64) (*CSVWriter, error) {
	res := new(CSVWriter)
	res.SchemaHandler = NewSchemaHandlerFromMetadata(md)
	res.Metadata = md
	res.PFile = pfile
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	res.PagesMapBuf = make(map[string][]*Layout.Page)
	res.NP = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.Offset = 4
	_, err := res.PFile.Write([]byte("PAR1"))
	return res, err
}

//Convert the column names to lowercase
func (self *CSVWriter) NameToLower() {
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

//Write string values to parquet file
func (self *CSVWriter) WriteString(recs []*string) {
	lr := len(recs)
	rec := make([]interface{}, lr)
	for i := 0; i < lr; i++ {
		rec[i] = nil
		if recs[i] != nil {
			rec[i] = ParquetType.StrToParquetType(*recs[i], self.Metadata[i].Type)
		}
	}

	ln := int64(len(self.Objs))
	if self.CheckSizeCritical <= ln {
		self.ObjSize = Common.SizeOf(reflect.ValueOf(rec))
	}
	self.ObjsSize += self.ObjSize
	self.Objs = append(self.Objs, rec)

	criSize := self.NP * self.PageSize * self.SchemaHandler.GetColumnNum()

	if self.ObjsSize > criSize {
		self.Flush(false)
	} else {
		dln := (criSize - self.ObjsSize + self.ObjSize - 1) / self.ObjSize / 2
		self.CheckSizeCritical = dln + ln
	}
}

//Write parquet values to parquet file
func (self *CSVWriter) Write(rec []interface{}) {
	ln := int64(len(self.Objs))
	if self.CheckSizeCritical <= ln {
		self.ObjSize = Common.SizeOf(reflect.ValueOf(rec))
	}

	self.ObjsSize += self.ObjSize
	self.Objs = append(self.Objs, rec)

	criSize := self.NP * self.PageSize * self.SchemaHandler.GetColumnNum()

	if self.ObjsSize > criSize {
		self.Flush(false)
	} else {
		dln := (criSize - self.ObjsSize + self.ObjSize - 1) / self.ObjSize / 2
		self.CheckSizeCritical = dln + ln
	}
}

//Write footer to parquet file and stop writing
func (self *CSVWriter) WriteStop() {
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
func (self *CSVWriter) Flush(flag bool) {
	pagesMapList := make([]map[string][]*Layout.Page, self.NP)
	for i := 0; i < int(self.NP); i++ {
		pagesMapList[i] = make(map[string][]*Layout.Page)
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

			tableMap := MarshalCSV(self.Objs, b, e, self.Metadata, self.SchemaHandler)
			for name, table := range *tableMap {
				pagesMapList[index][name], _ = Layout.TableToDataPages(table, int32(self.PageSize),
					parquet.CompressionCodec_SNAPPY)
			}

			doneChan <- 0
		}(int(bgn), int(end), c)
	}

	for c = 0; c < self.NP; c++ {
		<-doneChan
	}

	for _, pagesMap := range pagesMapList {
		for name, pages := range pagesMap {
			if _, ok := self.PagesMapBuf[name]; !ok {
				self.PagesMapBuf[name] = pages
			} else {
				self.PagesMapBuf[name] = append(self.PagesMapBuf[name], pages...)
			}
			for _, page := range pages {
				self.Size += int64(len(page.RawData))
				page.DataTable = nil
			}
		}
	}

	self.NumRows += int64(len(self.Objs))

	if self.Size+self.ObjsSize >= self.RowGroupSize || flag {
		//pages -> chunk
		chunkMap := make(map[string]*Layout.Chunk)
		for name, pages := range self.PagesMapBuf {
			chunkMap[name] = Layout.PagesToChunk(pages)
		}

		//chunks -> rowGroup
		rowGroup := Layout.NewRowGroup()
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
		rowGroup.RowGroupHeader.NumRows = self.NumRows
		self.NumRows = 0

		for k := 0; k < len(rowGroup.Chunks); k++ {
			rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = self.Offset
			rowGroup.Chunks[k].ChunkHeader.FileOffset = self.Offset

			for l := 0; l < len(rowGroup.Chunks[k].Pages); l++ {
				data := rowGroup.Chunks[k].Pages[l].RawData
				self.PFile.Write(data)
				self.Offset += int64(len(data))
			}
		}
		self.Footer.RowGroups = append(self.Footer.RowGroups, rowGroup.RowGroupHeader)
		self.Size = 0
		self.PagesMapBuf = make(map[string][]*Layout.Page)
	}
	self.Footer.NumRows += int64(len(self.Objs))
	self.Objs = self.Objs[:0]
	self.ObjsSize = 0
}
