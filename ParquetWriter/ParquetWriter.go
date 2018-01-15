package ParquetWriter

import (
	"encoding/binary"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/Marshal"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
	"strings"
	"sync"
)

//ParquetWriter is a writer  parquet file
type ParquetWriter struct {
	SchemaHandler *SchemaHandler.SchemaHandler
	NP            int64 //parallel number
	Footer        *parquet.FileMetaData
	PFile         ParquetFile.ParquetFile

	////write info/////
	PageSize     int64
	RowGroupSize int64
	Offset       int64

	Objs              []interface{}
	ObjsSize          int64
	ObjSize           int64
	CheckSizeCritical int64

	PagesMapBuf map[string][]*Layout.Page
	Size        int64
	NumRows     int64

	DictRecs map[string]*Layout.DictRecType
}

//Create a parquet handler
func NewParquetWriter(pFile ParquetFile.ParquetFile, obj interface{}, np int64) (*ParquetWriter, error) {
	var err error
	res := new(ParquetWriter)
	if res.SchemaHandler, err = SchemaHandler.NewSchemaHandlerFromStruct(obj); err != nil {
		return nil, err
	}
	res.NP = np
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	res.ObjsSize = 0
	res.CheckSizeCritical = 0
	res.Size = 0
	res.NumRows = 0
	res.Offset = 4
	res.PFile = pFile
	res.PagesMapBuf = make(map[string][]*Layout.Page)
	res.DictRecs = make(map[string]*Layout.DictRecType)

	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	_, err = res.PFile.Write([]byte("PAR1"))

	return res, err
}

//Convert the column names in schema to lowercases(Deprecated)
func (self *ParquetWriter) NameToLower() {
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

//Rename schema name to exname in tags
func (self *ParquetWriter) RenameSchema() {
	for i := 0; i < len(self.Footer.Schema); i++ {
		self.Footer.Schema[i].Name = self.SchemaHandler.Infos[i]["exname"].(string)
	}
	for _, rowGroup := range self.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			inPathStr := Common.PathToStr(chunk.MetaData.PathInSchema)
			exPathStr := self.SchemaHandler.InPathToExPath[inPathStr]
			exPath := Common.StrToPath(exPathStr)[1:]
			chunk.MetaData.PathInSchema = exPath
		}
	}
}

//Write the footer and stop writing
func (self *ParquetWriter) WriteStop() error {
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

	self.PFile.Write(footerBuf)
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

//Write one object to parquet file
func (self *ParquetWriter) Write(src interface{}) error {
	ln := int64(len(self.Objs))
	if self.CheckSizeCritical <= ln {
		self.ObjSize = Common.SizeOf(reflect.ValueOf(src))
	}
	self.ObjsSize += self.ObjSize
	self.Objs = append(self.Objs, src)

	criSize := self.NP * self.PageSize * self.SchemaHandler.GetColumnNum()

	if self.ObjsSize >= criSize {
		if err := self.Flush(false); err != nil {
			return err
		}

	} else {
		dln := (criSize - self.ObjsSize + self.ObjSize - 1) / self.ObjSize / 2
		self.CheckSizeCritical = dln + ln
	}
	return nil

}

//Flush the write buffer to parquet file
func (self *ParquetWriter) Flush(flag bool) (err error) {
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

			if tableMap, err := Marshal.Marshal(self.Objs, b, e, self.SchemaHandler); err == nil {
				for name, table := range *tableMap {
					if table.Info["encoding"] == parquet.Encoding_PLAIN_DICTIONARY {
						lock.Lock()
						if _, ok := self.DictRecs[name]; !ok {
							self.DictRecs[name] = Layout.NewDictRec()
						}
						pagesMapList[index][name], _ = Layout.TableToDictDataPages(self.DictRecs[name],
							table, int32(self.PageSize), 32, parquet.CompressionCodec_SNAPPY)
						lock.Unlock()

					} else {
						pagesMapList[index][name], _, _ = Layout.TableToDataPages(table, int32(self.PageSize),
							parquet.CompressionCodec_SNAPPY)
					}
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
				page.DataTable = nil //release memory
			}
		}
	}

	self.NumRows += int64(len(self.Objs))

	if self.Size+self.ObjsSize >= self.RowGroupSize || flag {
		//pages -> chunk
		chunkMap := make(map[string]*Layout.Chunk)
		for name, pages := range self.PagesMapBuf {
			if len(pages) > 0 && pages[0].Info["encoding"] == parquet.Encoding_PLAIN_DICTIONARY {
				dictPage, _ := Layout.DictRecToDictPage(self.DictRecs[name], int32(self.PageSize), parquet.CompressionCodec_SNAPPY)
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
			rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = -1
			rowGroup.Chunks[k].ChunkHeader.FileOffset = self.Offset

			for l := 0; l < len(rowGroup.Chunks[k].Pages); l++ {
				if rowGroup.Chunks[k].Pages[l].Header.Type == parquet.PageType_DICTIONARY_PAGE {
					tmp := self.Offset
					rowGroup.Chunks[k].ChunkHeader.MetaData.DictionaryPageOffset = &tmp
				} else if rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset <= 0 {
					rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = self.Offset

				}
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
