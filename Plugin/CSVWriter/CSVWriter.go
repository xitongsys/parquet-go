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
	"sync"
)

//Write handler for CSV data
type CSVWriter struct {
	SchemaHandler *SchemaHandler.SchemaHandler
	NP            int64
	Footer        *parquet.FileMetaData
	RowGroups     []*Layout.RowGroup
	Metadata      []string

	PFile ParquetFile.ParquetFile

	PageSize     int64
	RowGroupSize int64
	CompressType parquet.CompressionCodec
	Offset       int64

	Objs              [][]interface{}
	ObjsSize          int64
	ObjSize           int64
	CheckSizeCritical int64

	PagesMapBuf map[string][]*Layout.Page
	Size        int64
	NumRows     int64

	DictRecs map[string]*Layout.DictRecType
}

//Create CSV writer
func NewCSVWriter(md []string, pfile ParquetFile.ParquetFile, np int64) (*CSVWriter, error) {
	var err error
	res := new(CSVWriter)
	if res.SchemaHandler, err = NewSchemaHandlerFromMetadata(md); err != nil {
		return nil, err
	}
	res.Metadata = md
	res.PFile = pfile
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	res.CompressType = parquet.CompressionCodec_SNAPPY
	res.PagesMapBuf = make(map[string][]*Layout.Page)
	res.DictRecs = make(map[string]*Layout.DictRecType)
	res.NP = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.Offset = 4
	_, err = res.PFile.Write([]byte("PAR1"))
	return res, err
}

//Rename schema name to exname
func (self *CSVWriter) RenameSchema() {
	for _, rowGroup := range self.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			chunk.MetaData.PathInSchema = chunk.MetaData.PathInSchema[1:]
		}
	}
}

//Write string values to parquet file
func (self *CSVWriter) WriteString(recs []*string) error {
	lr := len(recs)
	rec := make([]interface{}, lr)
	for i := 0; i < lr; i++ {
		rec[i] = nil
		if recs[i] != nil {
			rec[i] = ParquetType.StrToParquetType(*recs[i],
				self.SchemaHandler.SchemaElements[i+1].Type,
				self.SchemaHandler.SchemaElements[i+1].ConvertedType,
				int(self.SchemaHandler.SchemaElements[i+1].GetTypeLength()),
				int(self.SchemaHandler.SchemaElements[i+1].GetScale()),
			)
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
		if err := self.Flush(false); err != nil {
			return err
		}
	} else {
		dln := (criSize - self.ObjsSize + self.ObjSize - 1) / self.ObjSize / 2
		self.CheckSizeCritical = dln + ln
	}
	return nil
}

//Write parquet values to parquet file
func (self *CSVWriter) Write(rec []interface{}) error {
	ln := int64(len(self.Objs))
	if self.CheckSizeCritical <= ln {
		self.ObjSize = Common.SizeOf(reflect.ValueOf(rec))
	}

	self.ObjsSize += self.ObjSize
	self.Objs = append(self.Objs, rec)

	criSize := self.NP * self.PageSize * self.SchemaHandler.GetColumnNum()

	if self.ObjsSize > criSize {
		if err := self.Flush(false); err != nil {
			return err
		}
	} else {
		dln := (criSize - self.ObjsSize + self.ObjSize - 1) / self.ObjSize / 2
		self.CheckSizeCritical = dln + ln
	}
	return nil
}

//Write footer to parquet file and stop writing
func (self *CSVWriter) WriteStop() error {
	var err error
	if err = self.Flush(true); err != nil {
		return err
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	self.RenameSchema()
	var footerBuf []byte
	if footerBuf, err = ts.Write(self.Footer); err != nil {
		return err
	}

	if _, err = self.PFile.Write(footerBuf); err != nil {
		return err
	}
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))
	if _, err = self.PFile.Write(footerSizeBuf); err != nil {
		return err
	}
	if _, err = self.PFile.Write([]byte("PAR1")); err != nil {
		return err
	}
	return nil
}

//Flush the write buffer to parquet file
func (self *CSVWriter) Flush(flag bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	pagesMapList := make([]map[string][]*Layout.Page, self.NP)
	for i := 0; i < int(self.NP); i++ {
		pagesMapList[i] = make(map[string][]*Layout.Page)
	}

	doneChan := make(chan int)
	l := int64(len(self.Objs))
	if l <= 0 {
		return
	}
	var c int64 = 0
	delta := (l + self.NP - 1) / self.NP
	lock := new(sync.Mutex)
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
			tableMap := MarshalCSV(self.Objs, b, e, self.SchemaHandler)
			for name, table := range *tableMap {
				if table.Info["encoding"] == parquet.Encoding_PLAIN_DICTIONARY {
					lock.Lock()
					if _, ok := self.DictRecs[name]; !ok {
						self.DictRecs[name] = Layout.NewDictRec()
					}
					pagesMapList[index][name], _ = Layout.TableToDictDataPages(self.DictRecs[name],
						table, int32(self.PageSize), 32, self.CompressType)
					lock.Unlock()
				} else {
					pagesMapList[index][name], _, _ = Layout.TableToDataPages(table, int32(self.PageSize),
						self.CompressType)
				}
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
			if len(pages) > 0 && pages[0].Info["encoding"] == parquet.Encoding_PLAIN_DICTIONARY {
				dictPage, _ := Layout.DictRecToDictPage(self.DictRecs[name], int32(self.PageSize), self.CompressType)
				tmp := append([]*Layout.Page{dictPage}, pages...)
				chunkMap[name] = Layout.PagesToDictChunk(tmp)
			} else {
				chunkMap[name] = Layout.PagesToChunk(pages)
			}
		}

		self.DictRecs = make(map[string]*Layout.DictRecType) //clean records for next chunks

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
	return nil
}
