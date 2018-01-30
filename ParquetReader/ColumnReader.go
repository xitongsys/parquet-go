package ParquetReader

import (
	"strings"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/SchemaHandler"
)

// NewParquetColumnReader creates a parquet column reader
func NewParquetColumnReader(pFile ParquetFile.ParquetFile, np int64) (*ParquetReader, error) {
	var err error
	res := new(ParquetReader)
	res.NP = np
	res.PFile = pFile
	res.ReadFooter()
	res.ColumnBuffers = make(map[string]*ColumnBufferType)
	res.SchemaHandler = SchemaHandler.NewSchemaHandlerFromSchemaList(res.Footer.GetSchema())

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

// ReadColumnByPath reads column by path in schema.
func (self *ParquetReader) ReadColumnByPath(pathStr string, num int) (values []interface{}, rls []int32, dls []int32) {
	if num <= 0 || len(pathStr) <= 0 {
		return []interface{}{}, []int32{}, []int32{}
	}
	rootName := self.SchemaHandler.GetRootName()
	if !strings.HasPrefix(pathStr, rootName) {
		pathStr = rootName + "." + pathStr
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
