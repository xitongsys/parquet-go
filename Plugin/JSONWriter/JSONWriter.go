package JSONWriter

import (
	"encoding/binary"
	"reflect"
	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

//Write handler for JSON data
type JSONWriter struct {
	SchemaHandler *SchemaHandler.SchemaHandler
	NP            int64
	Footer        *parquet.FileMetaData
	RowGroups     []*Layout.RowGroup

	PFile ParquetFile.ParquetFile

	PageSize     int64
	RowGroupSize int64
	CompressType parquet.CompressionCodec
	Offset       int64

	Objs              []string
	ObjsSize          int64
	ObjSize           int64
	CheckSizeCritical int64

	PagesMapBuf map[string][]*Layout.Page
	Size        int64
	NumRows     int64

	DictRecs map[string]*Layout.DictRecType
}

//Create JSON writer
func NewJSONWriter(jsonSchema string, pfile ParquetFile.ParquetFile, np int64) (*JSONWriter, error) {
	var err error
	res := new(JSONWriter)
	res.SchemaHandler, err = NewSchemaHandlerFromJSON(jsonSchema)
	if err != nil {
		return res, err
	}

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
func (self *JSONWriter) RenameSchema() {
	for _, rowGroup := range self.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			chunk.MetaData.PathInSchema = chunk.MetaData.PathInSchema[1:]
		}
	}
}

//Write parquet values to parquet file
func (self *JSONWriter) Write(rec string) error {
	var err error
	ln := int64(len(self.Objs))
	if self.CheckSizeCritical <= ln {
		self.ObjSize = Common.SizeOf(reflect.ValueOf(rec))
	}

	self.ObjsSize += self.ObjSize
	self.Objs = append(self.Objs, rec)

	criSize := self.NP * self.PageSize * self.SchemaHandler.GetColumnNum()

	if self.ObjsSize > criSize {
		err = self.Flush(false)
	} else {
		dln := (criSize - self.ObjsSize + self.ObjSize - 1) / self.ObjSize / 2
		self.CheckSizeCritical = dln + ln
	}
	return err
}

//Write footer to parquet file and stop writing
func (self *JSONWriter) WriteStop() error {
	var err error
	if err = self.Flush(true); err != nil {
		return err
	}
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	self.RenameSchema()
	footerBuf, err := ts.Write(self.Footer)
	if err != nil {
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

func (self *JSONWriter) flushObjs() error {
	var err error
	l := int64(len(self.Objs))
	if l <= 0 {
		return nil
	}

	pagesMapList := make([]map[string][]*Layout.Page, self.NP)
	for i := 0; i < int(self.NP); i++ {
		pagesMapList[i] = make(map[string][]*Layout.Page)
	}
	doneChan := make(chan int)

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
			tableMap, err2 := MarshalJSON(self.Objs, b, e, self.SchemaHandler)

			if err2 == nil {
				for name, table := range *tableMap {
					if table.Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY {
						lock.Lock()
						if _, ok := self.DictRecs[name]; !ok {
							self.DictRecs[name] = Layout.NewDictRec()
						}
						pagesMapList[index][name], _ = Layout.TableToDictDataPages(self.DictRecs[name],
							table, int32(self.PageSize), 32, self.CompressType)
						lock.Unlock()
					} else {
						pagesMapList[index][name], _ = Layout.TableToDataPages(table, int32(self.PageSize),
							self.CompressType)
					}
				}
			} else {
				err = err2
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
	return err
}

//Flush the write buffer to parquet file
func (self *JSONWriter) Flush(flag bool) error {
	var err error
	if err = self.flushObjs(); err != nil {
		return err
	}

	if (self.Size+self.ObjsSize >= self.RowGroupSize || flag) && len(self.PagesMapBuf) > 0 {
		//pages -> chunk
		chunkMap := make(map[string]*Layout.Chunk)
		for name, pages := range self.PagesMapBuf {
			if len(pages) > 0 && pages[0].Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY {
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
			if chunk == nil {
				continue
			}
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
				if _, err = self.PFile.Write(data); err != nil {
					return err
				}
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
