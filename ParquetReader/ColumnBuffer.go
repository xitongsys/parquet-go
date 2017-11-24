package ParquetReader

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
)

type ColumnBufferType struct {
	PFile        ParquetFile
	ThriftReader *thrift.TBufferedTransport

	Footer        *parquet.FileMetaData
	SchemaHandler *SchemaHandler.SchemaHandler

	PathStr       string
	RowGroupIndex int64
	ChunkHeader   *parquet.ColumnChunk

	ChunkReadValues int64

	DictPage *Page

	DataTable        *Common.Table
	DataTableNumRows int64
}

func NewColumnBuffer(pFile PFile, footer *parquet.FileMetaData, schemaHandler *SchemaHandler.SchemaHandler, pathStr string) *ColumnBufferType {
	return &ColumnBufferType{
		PFile:         pFile,
		Footer:        footer,
		SchemaHandler: schemaHandler,
		PathStr:       pathStr,
	}
}

func (self *ColumnBufferType) NextRowGroup() error {
	rowGroups := self.Footer.GetRowGroups()
	ln := int64(len(rowGroups))
	if self.RowGroupIndex >= ln {
		return fmt.Errorf("End of row groups")
	}
	self.RowGroupIndex++

	columnChunks := rowGroups[self.RowGroupIndex]
	i := int64(0)
	for i = 0; i < ln; i++ {
		path = append(path, self.SchemaHandler.GetRootName())
		path = append(path, columnChunks[i].MetaData.GetPathInSchema()...)
		if self.PathStr == Common.PathToStr(path) {
			break
		}
	}
	if i >= ln {
		return fmt.Errorf("Column not found")
	}

	self.ChunkHeader = columnChunks[i]
	if columnChunks[i].FilePath != nil {
		self.PFile.Close()
		self.PFile, _ = self.PFile.Open(*columnChunks[i].FilePath)
	}
	offset := columnChunks[i].FileOffset
	size := columnChunks[i].MetaData.GetTotalCompressedSize()
	self.ThriftReader = ConvertToThriftReader(self.PFile, offset, size)
	self.ChunkReadValues = 0
	self.DictPage = nil
	return nil
}

func (self *ColumnBufferType) ReadPage() error {
	if self.ChunkReadValues < self.ChunkHeader.MetaData.NumValues {
		page, numValues, numRows := Layout.ReadPage(self.ThriftReader, self.SchemaHandler, self.ChunkHeader.MetaData)
		if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
			self.DictPage = page
			return nil
		}
		page.Decode(self.DictPage)
		self.DataTable.Merge(page.DataTable)
		self.DataTableNumRows += numRows
		self.ChunkReadValues += numValues
	} else {
		err := self.NewRowGroup()
		if err != nil {
			return err
		}
		self.ReadPage()
	}
}

func (self *ColumnBufferType) ReadRows(dst interface{}) (*Common.Table, int) {
	var err error
	num := reflect.ValueOf(dst).Elem().Len()
	for self.DataTableNumRows < num && err == nil {
		err = self.ReadPage()
	}
	if num > self.DataTableNumRows {
		num = self.DataTableNumRows
	}
	return self.DataTable.Pop(num), num
}
