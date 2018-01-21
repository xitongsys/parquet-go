package ParquetReader

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

type ParquetReader struct {
	SchemaHandler *SchemaHandler.SchemaHandler
	NP            int64 //parallel number
	Footer        *parquet.FileMetaData
	PFile         ParquetFile.ParquetFile

	ColumnBuffers map[string]*ColumnBufferType
}

// NewParquetColumnReader creates a parquet column reader
func NewParquetColumnReader(pFile ParquetFile.ParquetFile, np int64) (*ParquetReader, error) {
	var err error
	res := &ParquetReader{
		NP:            np,
		PFile:         pFile,
		ColumnBuffers: make(map[string]*ColumnBufferType),
	}
	if err = res.ReadFooter(); err != nil {
		return nil, err
	}
	if res.SchemaHandler, err = SchemaHandler.NewSchemaHandlerFromSchemaList(res.Footer.GetSchema()); err != nil {
		return nil, err
	}
	for i := 0; i < len(res.SchemaHandler.SchemaElements); i++ {
		schema := res.SchemaHandler.SchemaElements[i]
		pathStr := res.SchemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			res.ColumnBuffers[pathStr], err = NewColumnBuffer(pFile, res.Footer, res.SchemaHandler, pathStr)
			if err != nil {
				return res, err
			}
		}
	}
	return res, nil
}

// NewParquetColumnReader creates a parquet reader
func NewParquetReader(pFile ParquetFile.ParquetFile, obj interface{}, np int64) (*ParquetReader, error) {
	var err error
	res := &ParquetReader{
		NP:            np,
		PFile:         pFile,
		ColumnBuffers: make(map[string]*ColumnBufferType),
	}
	if err = res.ReadFooter(); err != nil {
		return nil, err
	}
	if res.SchemaHandler, err = SchemaHandler.NewSchemaHandlerFromStruct(obj); err != nil {
		return nil, err
	}
	res.RenameSchema()

	for i := 0; i < len(res.SchemaHandler.SchemaElements); i++ {
		schema := res.SchemaHandler.SchemaElements[i]
		pathStr := res.SchemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			res.ColumnBuffers[pathStr], err = NewColumnBuffer(pFile, res.Footer, res.SchemaHandler, pathStr)
			if err != nil {
				return res, err
			}
		}
	}
	return res, nil
}

//Rename schema name to inname
func (self *ParquetReader) RenameSchema() {
	for i := 0; i < len(self.Footer.Schema); i++ {
		self.Footer.Schema[i].Name = self.SchemaHandler.Infos[i]["inname"].(string)
	}
	for _, rowGroup := range self.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			exPath := make([]string, 0)
			exPath = append(exPath, self.SchemaHandler.GetRootName())
			exPath = append(exPath, chunk.MetaData.GetPathInSchema()...)
			exPathStr := Common.PathToStr(exPath)

			inPathStr := self.SchemaHandler.ExPathToInPath[exPathStr]
			inPath := Common.StrToPath(inPathStr)[1:]
			chunk.MetaData.PathInSchema = inPath
		}
	}
}

func (self *ParquetReader) GetNumRows() int64 {
	return self.Footer.GetNumRows()
}

//Get the footer size
func (self *ParquetReader) GetFooterSize() (uint32, error) {
	buf := make([]byte, 4)
	if _, err := self.PFile.Seek(-8, 2); err != nil {
		return 0, err
	}
	if _, err := self.PFile.Read(buf); err != nil {
		return 0, err
	}
	size := binary.LittleEndian.Uint32(buf)
	return size, nil
}

//Read footer from parquet file
func (self *ParquetReader) ReadFooter() error {
	var (
		size uint32
		err  error
	)
	if size, err = self.GetFooterSize(); err != nil {
		return err
	}
	if _, err = self.PFile.Seek(int(-(int64)(8+size)), 2); err != nil {
		return err
	}
	self.Footer = parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactory()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(self.PFile))
	if err = self.Footer.Read(protocol); err != nil {
		return err
	}
	return nil
}

//Read rows of parquet file
func (self *ParquetReader) Read(dstInterface interface{}) (err error) {
	tmap := make(map[string]*Layout.Table)
	locker := new(sync.Mutex)
	ot := reflect.TypeOf(dstInterface).Elem().Elem()
	num := reflect.ValueOf(dstInterface).Elem().Len()
	if num <= 0 {
		return
	}

	doneChan := make(chan int, self.NP)
	taskChan := make(chan string, len(self.ColumnBuffers))
	stopChan := make(chan int)

	for i := int64(0); i < self.NP; i++ {
		go func() {
			for {
				select {
				case <-stopChan:
					return
				case pathStr := <-taskChan:
					cb := self.ColumnBuffers[pathStr]
					table, _, _ := cb.ReadRows(int64(num))
					locker.Lock()
					if _, ok := tmap[pathStr]; ok {
						tmap[pathStr].Merge(table)
					} else {
						tmap[pathStr] = Layout.NewTableFromTable(table)
						tmap[pathStr].Merge(table)
					}
					locker.Unlock()
					doneChan <- 0
				}
			}
		}()
	}
	for key, _ := range self.ColumnBuffers {
		taskChan <- key
	}
	for i := 0; i < len(self.ColumnBuffers); i++ {
		<-doneChan
	}
	for i := int64(0); i < self.NP; i++ {
		stopChan <- 0
	}

	dstList := make([]interface{}, self.NP)
	delta := (int64(num) + self.NP - 1) / self.NP

	doneChan = make(chan int)
	for c := int64(0); c < self.NP; c++ {
		bgn := c * delta
		end := bgn + delta
		if end > int64(num) {
			end = int64(num)
		}
		if bgn >= int64(num) {
			bgn, end = int64(num), int64(num)
		}
		go func(b, e, index int) {
			dstList[index] = reflect.New(reflect.SliceOf(ot)).Interface()
			if r := Marshal.Unmarshal(&tmap, b, e, dstList[index], self.SchemaHandler); r != nil {
				err = r
			}
			doneChan <- 0
		}(int(bgn), int(end), int(c))
	}
	for c := int64(0); c < self.NP; c++ {
		<-doneChan
	}

	resTmp := reflect.MakeSlice(reflect.SliceOf(ot), 0, num)
	for _, dst := range dstList {
		resTmp = reflect.AppendSlice(resTmp, reflect.ValueOf(dst).Elem())
	}
	reflect.ValueOf(dstInterface).Elem().Set(resTmp)

	return nil
}

//Read column by path in schema.
func (self *ParquetReader) ReadColumnByPath(pathStr string, dstInterface *[]interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	num := reflect.ValueOf(dstInterface).Elem().Len()
	if num <= 0 {
		return
	}
	rootName := self.SchemaHandler.GetRootName()

	if len(pathStr) <= 0 {
		return
	} else if !strings.HasPrefix(pathStr, rootName) {
		pathStr = rootName + "." + pathStr
	}

	if cb, ok := self.ColumnBuffers[pathStr]; ok {
		table, _, _ := cb.ReadRows(int64(num))
		i, j, ln := 0, 0, len(table.Values)

		numi := 0
		for i < ln {
			rec := [][]interface{}{}
			j = i
			for j < ln {
				rec = append(rec, []interface{}{table.Values[j], table.RepetitionLevels[j], table.DefinitionLevels[j]})
				j++
				if j < ln && table.RepetitionLevels[j] == 0 {
					break
				}
			}
			(*dstInterface)[numi] = rec
			numi++
			i = j
		}
	}
	return nil
}

//Read column by index. The index of first column is 0.
func (self *ParquetReader) ReadColumnByIndex(index int, dstInterface *[]interface{}) error {
	if index >= len(self.SchemaHandler.ValueColumns) {
		return nil
	}
	pathStr := self.SchemaHandler.ValueColumns[index]
	err := self.ReadColumnByPath(pathStr, dstInterface)
	return err
}

//Stop Read
func (self *ParquetReader) ReadStop() {
	for _, cb := range self.ColumnBuffers {
		if cb != nil {
			cb.PFile.Close()
		}
	}
}
