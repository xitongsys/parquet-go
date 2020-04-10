package reader

import (
	"encoding/binary"
	"io"
	"reflect"
	"sync"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/parquet"
)

type ParquetReader struct {
	SchemaHandler *schema.SchemaHandler
	NP            int64 //parallel number
	Footer        *parquet.FileMetaData
	PFile         source.ParquetFile

	ColumnBuffers map[string]*ColumnBufferType

	//One reader can only read one type objects
	ObjType			reflect.Type
	ObjPartialType	reflect.Type
}

//Create a parquet reader: obj is a object with schema tags or a JSON schema string
func NewParquetReader(pFile source.ParquetFile, obj interface{}, np int64) (*ParquetReader, error) {
	var err error
	res := new(ParquetReader)
	res.NP = np
	res.PFile = pFile
	if err = res.ReadFooter(); err != nil {
		return nil, err
	}
	res.ColumnBuffers = make(map[string]*ColumnBufferType)

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

	}else{
		res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(res.Footer.Schema)
	}

	res.RenameSchema()
	for i := 0; i < len(res.SchemaHandler.SchemaElements); i++ {
		schema := res.SchemaHandler.SchemaElements[i]
		if schema.GetNumChildren() == 0 {
			pathStr := res.SchemaHandler.IndexMap[int32(i)]
			if res.ColumnBuffers[pathStr], err = NewColumnBuffer(pFile, res.Footer, res.SchemaHandler, pathStr); err != nil {
				return res, err
			}
		}
	}

	return res, nil
}

func (self *ParquetReader) SetSchemaHandlerFromJSON(jsonSchema string) error {
	var err error

	if self.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema); err != nil {
		return err
	}

	
	self.RenameSchema()
	for i := 0; i < len(self.SchemaHandler.SchemaElements); i++ {
		schemaElement := self.SchemaHandler.SchemaElements[i]
		if schemaElement.GetNumChildren() == 0 {
			pathStr := self.SchemaHandler.IndexMap[int32(i)]
			if self.ColumnBuffers[pathStr], err = NewColumnBuffer(self.PFile, self.Footer, self.SchemaHandler, pathStr); err != nil {
				return err
			}
		}
	}
	return nil
}

//Rename schema name to inname
func (self *ParquetReader) RenameSchema() {
	for i := 0; i < len(self.SchemaHandler.Infos); i++ {
		self.Footer.Schema[i].Name = self.SchemaHandler.Infos[i].InName
	}
	for _, rowGroup := range self.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			exPath := make([]string, 0)
			exPath = append(exPath, self.SchemaHandler.GetRootExName())
			exPath = append(exPath, chunk.MetaData.GetPathInSchema()...)
			exPathStr := common.PathToStr(exPath)

			inPathStr := self.SchemaHandler.ExPathToInPath[exPathStr]
			inPath := common.StrToPath(inPathStr)[1:]
			chunk.MetaData.PathInSchema = inPath
		}
	}
}

func (self *ParquetReader) GetNumRows() int64 {
	return self.Footer.GetNumRows()
}

//Get the footer size
func (self *ParquetReader) GetFooterSize() (uint32, error) {
	var err error
	buf := make([]byte, 4)
	if _, err = self.PFile.Seek(-8, io.SeekEnd); err != nil {
		return 0, err
	}
	if _, err = self.PFile.Read(buf); err != nil {
		return 0, err
	}
	size := binary.LittleEndian.Uint32(buf)
	return size, err
}

//Read footer from parquet file
func (self *ParquetReader) ReadFooter() error {
	size, err := self.GetFooterSize()
	if err != nil {
		return err
	}
	if _, err = self.PFile.Seek(-(int64)(8+size), io.SeekEnd); err != nil {
		return err
	}
	self.Footer = parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactory()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(self.PFile))
	return self.Footer.Read(protocol)
}

//Skip rows of parquet file
func (self *ParquetReader) SkipRows(num int64) error {
	var err error
	if num <= 0 {
		return nil
	}
	doneChan := make(chan int, self.NP)
	taskChan := make(chan string, len(self.SchemaHandler.ValueColumns))
	stopChan := make(chan int)

	for _, pathStr := range self.SchemaHandler.ValueColumns {
		if _, ok := self.ColumnBuffers[pathStr]; !ok {
			if self.ColumnBuffers[pathStr], err = NewColumnBuffer(self.PFile, self.Footer, self.SchemaHandler, pathStr); err != nil {
				return err
			}
		}
	}

	for i := int64(0); i < self.NP; i++ {
		go func() {
			for {
				select {
				case <-stopChan:
					return
				case pathStr := <-taskChan:
					cb := self.ColumnBuffers[pathStr]
					cb.SkipRows(int64(num))
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
	return err
}

//Read rows of parquet file and unmarshal all to dst
func (self *ParquetReader) Read(dstInterface interface{}) error {
	return self.read(dstInterface, "")
}

// Read maxReadNumber objects
func (self *ParquetReader) ReadByNumber(maxReadNumber int) ([]interface{}, error) {
	var err error 
	if self.ObjType == nil {
		if self.ObjType, err = self.SchemaHandler.GetType(self.SchemaHandler.GetRootInName()); err != nil {
			return nil, err
		}
	}

	vs := reflect.MakeSlice(reflect.SliceOf(self.ObjType), maxReadNumber, maxReadNumber)
	res := reflect.New(vs.Type())
	res.Elem().Set(vs)

	if err = self.Read(res.Interface()); err != nil {
		return nil, err
	}

	ln := res.Elem().Len()
	ret := make([]interface{}, ln)
	for i := 0; i < ln; i++ {
		ret[i] = res.Elem().Index(i).Interface()
	}

	return ret, nil
}

//Read rows of parquet file and unmarshal all to dst
func (self *ParquetReader) ReadPartial(dstInterface interface{}, prefixPath string) error {
	prefixPath, err := self.SchemaHandler.ConvertToInPathStr(prefixPath)
	if err != nil {
		return err
	}
	
	return self.read(dstInterface, prefixPath)
}

// Read maxReadNumber partial objects 
func (self *ParquetReader) ReadPartialByNumber(maxReadNumber int, prefixPath string) ([]interface{}, error) {
	var err error 
	if self.ObjPartialType == nil {
		if self.ObjPartialType, err = self.SchemaHandler.GetType(prefixPath); err != nil {
			return nil, err
		}
	}

	vs := reflect.MakeSlice(reflect.SliceOf(self.ObjPartialType), maxReadNumber, maxReadNumber)
	res := reflect.New(vs.Type())
	res.Elem().Set(vs)

	if err = self.ReadPartial(res.Interface(), prefixPath); err != nil {
		return nil, err
	}

	ln := res.Elem().Len()
	ret := make([]interface{}, ln)
	for i := 0; i < ln; i++ {
		ret[i] = res.Elem().Index(i).Interface()
	}

	return ret, nil
}

//Read rows of parquet file with a prefixPath
func (self *ParquetReader) read(dstInterface interface{}, prefixPath string) error {
	var err error
	tmap := make(map[string]*layout.Table)
	locker := new(sync.Mutex)
	ot := reflect.TypeOf(dstInterface).Elem().Elem()
	num := reflect.ValueOf(dstInterface).Elem().Len()
	if num <= 0 {
		return nil
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
					table, _ := cb.ReadRows(int64(num))
					locker.Lock()
					if _, ok := tmap[pathStr]; ok {
						tmap[pathStr].Merge(table)
					} else {
						tmap[pathStr] = layout.NewTableFromTable(table)
						tmap[pathStr].Merge(table)
					}
					locker.Unlock()
					doneChan <- 0
				}
			}
		}()
	}

	readNum := 0
	for key, _ := range self.ColumnBuffers {
		if strings.HasPrefix(key, prefixPath) {
			taskChan <- key
			readNum++
		}
	}
	for i := 0; i < readNum; i++ {
		<-doneChan
	}

	for i := int64(0); i < self.NP; i++ {
		stopChan <- 0
	}

	dstList := make([]interface{}, self.NP)
	delta := (int64(num) + self.NP - 1) / self.NP

	var wg sync.WaitGroup
	for c := int64(0); c < self.NP; c++ {
		bgn := c * delta
		end := bgn + delta
		if end > int64(num) {
			end = int64(num)
		}
		if bgn >= int64(num) {
			bgn, end = int64(num), int64(num)
		}
		wg.Add(1)
		go func(b, e, index int) {
			defer func(){
				wg.Done()
			}()

			dstList[index] = reflect.New(reflect.SliceOf(ot)).Interface()
			if err2 := marshal.Unmarshal(&tmap, b, e, dstList[index], self.SchemaHandler, prefixPath); err2 != nil {
				err = err2
			}
		}(int(bgn), int(end), int(c))
	}

	wg.Wait()

	resTmp := reflect.MakeSlice(reflect.SliceOf(ot), 0, num)
	for _, dst := range dstList {
		resTmp = reflect.AppendSlice(resTmp, reflect.ValueOf(dst).Elem())
	}
	reflect.ValueOf(dstInterface).Elem().Set(resTmp)
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
