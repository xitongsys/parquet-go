package ParquetReader

import (
	"strings"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/SchemaHandler"
)

// NewParquetColumnReader creates a parquet column reader
func NewParquetColumnReader(pFile ParquetFile.ParquetFile, np int64) (*ParquetReader, error) {
	res := new(ParquetReader)
	res.NP = np
	res.PFile = pFile
	if err := res.ReadFooter(); err != nil {
		return nil, err
	}
	res.ColumnBuffers = make(map[string]*ColumnBufferType)
	res.SchemaHandler = SchemaHandler.NewSchemaHandlerFromSchemaList(res.Footer.GetSchema())

	/* open when need
	for i := 0; i < len(res.SchemaHandler.SchemaElements); i++ {
		schema := res.SchemaHandler.SchemaElements[i]
		if schema.GetNumChildren() == 0 {
			pathStr := res.SchemaHandler.IndexMap[int32(i)]
			if res.ColumnBuffers[pathStr], err = NewColumnBuffer(pFile, res.Footer, res.SchemaHandler, pathStr); err != nil {
				return res, err
			}
		}
	}
	*/
	return res, nil
}

func (self *ParquetReader) SkipRowsByPath(pathStr string, num int) {
	if num <= 0 || len(pathStr) <= 0 {
		return
	}
	rootName := self.SchemaHandler.GetRootName()
	if !strings.HasPrefix(pathStr, rootName) {
		pathStr = rootName + "." + pathStr
	}

	if _, ok := self.SchemaHandler.MapIndex[pathStr]; !ok {
		return
	}

	if _, ok := self.ColumnBuffers[pathStr]; !ok {
		var err error
		if self.ColumnBuffers[pathStr], err = NewColumnBuffer(self.PFile, self.Footer, self.SchemaHandler, pathStr); err != nil {
			return
		}
	}

	if cb, ok := self.ColumnBuffers[pathStr]; ok {
		cb.SkipRows(int64(num))
	}
}

func (self *ParquetReader) SkipRowsByIndex(index int, num int) {
	if index >= len(self.SchemaHandler.ValueColumns) {
		return
	}
	pathStr := self.SchemaHandler.ValueColumns[index]
	self.SkipRowsByPath(pathStr, num)
}

// ReadColumnByPath reads column by path in schema.
func (self *ParquetReader) ReadColumnByPath(pathStr string, num int) (values []interface{}, rls []int32, dls []int32) {
	if num <= 0 || len(pathStr) <= 0 {
		return []interface{}{}, []int32{}, []int32{}
	}
	rootName := self.SchemaHandler.GetRootName()
	if !strings.HasPrefix(pathStr, rootName) {
		pathStr = rootName + "." + pathStr
	}

	if _, ok := self.SchemaHandler.MapIndex[pathStr]; !ok {
		return []interface{}{}, []int32{}, []int32{}
	}

	if _, ok := self.ColumnBuffers[pathStr]; !ok {
		var err error
		if self.ColumnBuffers[pathStr], err = NewColumnBuffer(self.PFile, self.Footer, self.SchemaHandler, pathStr); err != nil {
			return []interface{}{}, []int32{}, []int32{}
		}
	}

	if cb, ok := self.ColumnBuffers[pathStr]; ok {
		table, _ := cb.ReadRows(int64(num))
		return table.Values, table.RepetitionLevels, table.DefinitionLevels
	}
	return []interface{}{}, []int32{}, []int32{}
}

// ReadColumnByIndex reads column by index. The index of first column is 0.
func (self *ParquetReader) ReadColumnByIndex(index int, num int) (values []interface{}, rls []int32, dls []int32) {
	if index >= len(self.SchemaHandler.ValueColumns) {
		return
	}
	pathStr := self.SchemaHandler.ValueColumns[index]
	return self.ReadColumnByPath(pathStr, num)
}
