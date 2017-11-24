package ParquetReader

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

type ColumnBufferType struct {
	PFile        ParquetFile.ParquetFile
	ThriftReader *thrift.TBufferedTransport

	Footer        *parquet.FileMetaData
	SchemaHandler *SchemaHandler.SchemaHandler

	PathStr       string
	RowGroupIndex int64
	ChunkHeader   *parquet.ColumnChunk

	ChunkReadValues int64

	DictPage *Layout.Page

	DataTable        *Common.Table
	DataTableNumRows int64
}

func NewColumnBuffer(pFile ParquetFile.ParquetFile, footer *parquet.FileMetaData, schemaHandler *SchemaHandler.SchemaHandler, pathStr string) (*ColumnBufferType, error) {
	newPFile, err := pFile.Open("")
	return &ColumnBufferType{
		PFile:         newPFile,
		Footer:        footer,
		SchemaHandler: schemaHandler,
		PathStr:       pathStr,
	}, err
}

func (self *ColumnBufferType) NextRowGroup() error {
	rowGroups := self.Footer.GetRowGroups()
	ln := int64(len(rowGroups))
	if self.RowGroupIndex >= ln {
		return fmt.Errorf("End of row groups")
	}
	self.RowGroupIndex++

	columnChunks := rowGroups[self.RowGroupIndex].GetColumns()
	i := int64(0)
	for i = 0; i < ln; i++ {
		path := make([]string, 0)
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
	self.ThriftReader = ParquetFile.ConvertToThriftReader(self.PFile, offset, size)
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
		err := self.NextRowGroup()
		if err != nil {
			return err
		}
		self.ReadPage()
	}
	return nil
}

func (self *ColumnBufferType) ReadRows(num int64) (*Common.Table, int64) {
	var err error
	for self.DataTableNumRows < num && err == nil {
		err = self.ReadPage()
	}
	if num > self.DataTableNumRows {
		num = self.DataTableNumRows
	}
	return self.DataTable.Pop(num), num
}
