package ParquetReader

import (
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"strings"
)

//Create a parquet column reader
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
		pathStr := res.SchemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			res.ColumnBuffers[pathStr], err = NewColumnBuffer(pFile, res.Footer, res.SchemaHandler, pathStr)
			if err != nil {
				return res, err
			}
		}
	}
	return res, err
}

//Read column by path in schema.
func (self *ParquetReader) ReadColumnByPath(pathStr string, num int) (values []interface{}, rls []int32, dls []int32) {
	if num <= 0 {
		return []interface{}{}, []int32{}, []int32{}
	}

	rootName := self.SchemaHandler.GetRootName()

	if len(pathStr) <= 0 {
		return []interface{}{}, []int32{}, []int32{}
	} else if !strings.HasPrefix(pathStr, rootName) {
		pathStr = rootName + "." + pathStr
	}

	if cb, ok := self.ColumnBuffers[pathStr]; ok {
		table, _ := cb.ReadRows(int64(num))
		return table.Values, table.RepetitionLevels, table.DefinitionLevels
	}
	return []interface{}{}, []int32{}, []int32{}
}

//Read column by index. The index of first column is 0.
func (self *ParquetReader) ReadColumnByIndex(index int, num int) (values []interface{}, rls []int32, dls []int32) {
	if index >= len(self.SchemaHandler.ValueColumns) {
		return
	}
	pathStr := self.SchemaHandler.ValueColumns[index]
	return self.ReadColumnByPath(pathStr, num)
}

//Read column by path in schema.
func (self *ParquetReader) ReadColumnByPathDeprecated(pathStr string, dstInterface *[]interface{}) {
	num := len(*dstInterface)
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
		table, _ := cb.ReadRows(int64(num))
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
}

//Read column by index. The index of first column is 0.
func (self *ParquetReader) ReadColumnByIndexDeprecated(index int, dstInterface *[]interface{}) {
	if index >= len(self.SchemaHandler.ValueColumns) {
		return
	}
	pathStr := self.SchemaHandler.ValueColumns[index]
	self.ReadColumnByPathDeprecated(pathStr, dstInterface)
}
