package writer

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/source"
)

//ParquetWriter is a writer  parquet file
type ParquetWriter struct {
	SchemaHandler *schema.SchemaHandler
	NP            int64 //parallel number
	Footer        *parquet.FileMetaData
	PFile         source.ParquetFile

	PageSize        int64
	RowGroupSize    int64
	CompressionType parquet.CompressionCodec
	Offset          int64

	Objs              []interface{}
	ObjsSize          int64
	ObjSize           int64
	CheckSizeCritical int64

	PagesMapBuf map[string][]*layout.Page
	Size        int64
	NumRows     int64

	DictRecs map[string]*layout.DictRecType

	MarshalFunc func(src []interface{}, bgn int, end int, sh *schema.SchemaHandler) (*map[string]*layout.Table, error)
}

//Create a parquet handler. Obj is a object with tags or JSON schema string.
func NewParquetWriter(pFile source.ParquetFile, obj interface{}, np int64) (*ParquetWriter, error) {
	var err error

	res := new(ParquetWriter)
	res.NP = np
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	res.CompressionType = parquet.CompressionCodec_SNAPPY
	res.ObjsSize = 0
	res.CheckSizeCritical = 0
	res.Size = 0
	res.NumRows = 0
	res.Offset = 4
	res.PFile = pFile
	res.PagesMapBuf = make(map[string][]*layout.Page)
	res.DictRecs = make(map[string]*layout.DictRecType)
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	//include the createdBy to avoid 
	//WARN  CorruptStatistics:118 - Ignoring statistics because created_by is null or empty! See PARQUET-251 and PARQUET-297
	createdBy := "parquet-go version latest"
	res.Footer.CreatedBy = &createdBy
	_, err = res.PFile.Write([]byte("PAR1"))
	res.MarshalFunc = marshal.Marshal

	if obj != nil {
		if sa, ok := obj.(string); ok {
			err = res.SetSchemaHandlerFromJSON(sa)
			return res, err

		} else if sa, ok := obj.([]*parquet.SchemaElement); ok {
			res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(sa)

		} else {
			if res.SchemaHandler, err = schema.NewSchemaHandlerFromStruct(obj); err != nil {
				return res, err
			}
		}

		res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	}

	return res, err
}

func (self *ParquetWriter) SetSchemaHandlerFromJSON(jsonSchema string) error {
	var err error
	if self.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema); err != nil {
		return err
	}
	self.Footer.Schema = self.Footer.Schema[:0]
	self.Footer.Schema = append(self.Footer.Schema, self.SchemaHandler.SchemaElements...)
	return nil
}

//Rename schema name to exname in tags
func (self *ParquetWriter) RenameSchema() {
	for i := 0; i < len(self.Footer.Schema); i++ {
		self.Footer.Schema[i].Name = self.SchemaHandler.Infos[i].ExName
	}
	for _, rowGroup := range self.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			inPathStr := common.PathToStr(chunk.MetaData.PathInSchema)
			exPathStr := self.SchemaHandler.InPathToExPath[inPathStr]
			exPath := common.StrToPath(exPathStr)[1:]
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
	footerBuf, err := ts.Write(context.TODO(), self.Footer)
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

//Write one object to parquet file
func (self *ParquetWriter) Write(src interface{}) error {
	var err error
	ln := int64(len(self.Objs))

	val := reflect.ValueOf(src)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		src = val.Interface()
	}

	if self.CheckSizeCritical <= ln {
		self.ObjSize = (self.ObjSize+common.SizeOf(val))/2 + 1
	}
	self.ObjsSize += self.ObjSize
	self.Objs = append(self.Objs, src)

	criSize := self.NP * self.PageSize * self.SchemaHandler.GetColumnNum()

	if self.ObjsSize >= criSize {
		err = self.Flush(false)

	} else {
		dln := (criSize - self.ObjsSize + self.ObjSize - 1) / self.ObjSize / 2
		self.CheckSizeCritical = dln + ln
	}
	return err

}

func (self *ParquetWriter) flushObjs() error {
	var err error
	l := int64(len(self.Objs))
	if l <= 0 {
		return nil
	}
	pagesMapList := make([]map[string][]*layout.Page, self.NP)
	for i := 0; i < int(self.NP); i++ {
		pagesMapList[i] = make(map[string][]*layout.Page)
	}

	var c int64 = 0
	delta := (l + self.NP - 1) / self.NP
	lock := new(sync.Mutex)
	var wg sync.WaitGroup
	for c = 0; c < self.NP; c++ {
		bgn := c * delta
		end := bgn + delta
		if end > l {
			end = l
		}
		if bgn >= l {
			bgn, end = l, l
		}

		wg.Add(1)
		go func(b, e int, index int64) {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					switch x := r.(type) {
					case string:
						err = errors.New(x)
					case error:
						err = x
					default:
						err = errors.New("unknown error")
					}
				}
			}()

			if e <= b {
				return
			}

			tableMap, err2 := self.MarshalFunc(self.Objs, b, e, self.SchemaHandler)

			if err2 == nil {
				for name, table := range *tableMap {
					if table.Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY ||
						table.Info.Encoding == parquet.Encoding_RLE_DICTIONARY {
						lock.Lock()
						if _, ok := self.DictRecs[name]; !ok {
							self.DictRecs[name] = layout.NewDictRec(*table.Schema.Type)
						}
						pagesMapList[index][name], _ = layout.TableToDictDataPages(self.DictRecs[name],
							table, int32(self.PageSize), 32, self.CompressionType)
						lock.Unlock()

					} else {
						pagesMapList[index][name], _ = layout.TableToDataPages(table, int32(self.PageSize),
							self.CompressionType)
					}
				}
			} else {
				err = err2
			}

		}(int(bgn), int(end), c)
	}

	wg.Wait()

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
	return err
}

//Flush the write buffer to parquet file
func (self *ParquetWriter) Flush(flag bool) error {
	var err error

	if err = self.flushObjs(); err != nil {
		return err
	}

	if (self.Size+self.ObjsSize >= self.RowGroupSize || flag) && len(self.PagesMapBuf) > 0 {
		//pages -> chunk
		chunkMap := make(map[string]*layout.Chunk)
		for name, pages := range self.PagesMapBuf {
			if len(pages) > 0 && (pages[0].Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY || pages[0].Info.Encoding == parquet.Encoding_RLE_DICTIONARY) {
				dictPage, _ := layout.DictRecToDictPage(self.DictRecs[name], int32(self.PageSize), self.CompressionType)
				tmp := append([]*layout.Page{dictPage}, pages...)
				chunkMap[name] = layout.PagesToDictChunk(tmp)
			} else {
				chunkMap[name] = layout.PagesToChunk(pages)

			}
		}

		self.DictRecs = make(map[string]*layout.DictRecType) //clean records for next chunks

		//chunks -> rowGroup
		rowGroup := layout.NewRowGroup()
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
			//rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalCompressedSize
			rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalUncompressedSize
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
				if _, err = self.PFile.Write(data); err != nil {
					return err
				}
				self.Offset += int64(len(data))
			}
		}
		self.Footer.RowGroups = append(self.Footer.RowGroups, rowGroup.RowGroupHeader)
		self.Size = 0
		self.PagesMapBuf = make(map[string][]*layout.Page)
	}
	self.Footer.NumRows += int64(len(self.Objs))
	self.Objs = self.Objs[:0]
	self.ObjsSize = 0
	return nil

}
