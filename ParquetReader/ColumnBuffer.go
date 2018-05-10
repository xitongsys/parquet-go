package ParquetReader

import (
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
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

	DataTable        *Layout.Table
	DataTableNumRows int64
}

func NewColumnBuffer(pFile ParquetFile.ParquetFile, footer *parquet.FileMetaData, schemaHandler *SchemaHandler.SchemaHandler, pathStr string) (*ColumnBufferType, error) {
	newPFile, err := pFile.Open("")
	if err != nil {
		return nil, err
	}
	res := &ColumnBufferType{
		PFile:            newPFile,
		Footer:           footer,
		SchemaHandler:    schemaHandler,
		PathStr:          pathStr,
		DataTableNumRows: -1,
	}
	err = res.NextRowGroup()
	return res, err
}

func (self *ColumnBufferType) NextRowGroup() error {
	var err error
	rowGroups := self.Footer.GetRowGroups()
	ln := int64(len(rowGroups))
	if self.RowGroupIndex >= ln {
		self.DataTableNumRows++ //very important, because DataTableNumRows is one smaller than real rows number
		return fmt.Errorf("End of row groups")
	}
	self.RowGroupIndex++

	columnChunks := rowGroups[self.RowGroupIndex-1].GetColumns()
	i := int64(0)
	ln = int64(len(columnChunks))
	for i = 0; i < ln; i++ {
		path := make([]string, 0)
		path = append(path, self.SchemaHandler.GetRootName())
		path = append(path, columnChunks[i].MetaData.GetPathInSchema()...)
		if self.PathStr == Common.PathToStr(path) {
			break
		}
	}
	if i >= ln {
		return fmt.Errorf("Column not found: %v", self.PathStr)
	}

	self.ChunkHeader = columnChunks[i]
	if columnChunks[i].FilePath != nil {
		self.PFile.Close()
		if self.PFile, err = self.PFile.Open(*columnChunks[i].FilePath); err != nil {
			return err
		}
	}
	//offset := columnChunks[i].FileOffset
	offset := columnChunks[i].MetaData.DataPageOffset
	if columnChunks[i].MetaData.DictionaryPageOffset != nil {
		offset = *columnChunks[i].MetaData.DictionaryPageOffset
	}
	size := columnChunks[i].MetaData.GetTotalCompressedSize()
	if self.ThriftReader != nil {
		self.ThriftReader.Close()
	}
	self.ThriftReader = ParquetFile.ConvertToThriftReader(self.PFile, offset, size)
	self.ChunkReadValues = 0
	self.DictPage = nil
	return nil
}

func (self *ColumnBufferType) ReadPage() error {
	if self.ChunkReadValues < self.ChunkHeader.MetaData.NumValues {
		page, numValues, numRows, err := Layout.ReadPage(self.ThriftReader, self.SchemaHandler, self.ChunkHeader.MetaData)
		if err != nil {
			//data is nil and rl/dl=0, no pages in file
			if err == io.EOF {
				if self.DataTable == nil {
					index := self.SchemaHandler.MapIndex[self.PathStr]
					self.DataTable = Layout.NewEmptyTable()
					self.DataTable.Type = self.SchemaHandler.SchemaElements[index].GetType()
					self.DataTable.Path = Common.StrToPath(self.PathStr)

				}
				for self.ChunkReadValues < self.ChunkHeader.MetaData.NumValues {
					self.DataTable.Values = append(self.DataTable.Values, nil)
					self.DataTable.RepetitionLevels = append(self.DataTable.RepetitionLevels, int32(0))
					self.DataTable.DefinitionLevels = append(self.DataTable.DefinitionLevels, int32(0))
					self.ChunkReadValues++
					self.DataTableNumRows++
				}
			}
			return err
		}
		if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
			self.DictPage = page
			return nil
		}

		page.Decode(self.DictPage)

		if self.DataTable == nil {
			self.DataTable = Layout.NewTableFromTable(page.DataTable)
		}
		self.DataTable.Merge(page.DataTable)
		self.ChunkReadValues += numValues

		self.DataTableNumRows += numRows
	} else {
		if err := self.NextRowGroup(); err != nil {
			return err
		}
		return self.ReadPage()
	}

	return nil
}

func (self *ColumnBufferType) ReadPageForSkip() (*Layout.Page, error) {
	if self.ChunkReadValues < self.ChunkHeader.MetaData.NumValues {
		page, err := Layout.ReadPageRawData(self.ThriftReader, self.SchemaHandler, self.ChunkHeader.MetaData)
		if err != nil {
			return nil, err
		}
		numValues, numRows, err := page.GetRLDLFromRawData(self.SchemaHandler)
		if err != nil {
			return nil, err
		}
		if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
			page.GetValueFromRawData(self.SchemaHandler)
			self.DictPage = page
			return page, nil
		}
		if self.DataTable == nil {
			self.DataTable = Layout.NewTableFromTable(page.DataTable)
		}
		self.DataTable.Merge(page.DataTable)
		self.ChunkReadValues += numValues
		self.DataTableNumRows += numRows
		return page, nil

	} else {
		if err := self.NextRowGroup(); err != nil {
			return nil, err
		}
		return self.ReadPageForSkip()
	}
}

func (self *ColumnBufferType) SkipRows(num int64) int64 {
	var (
		err  error
		page *Layout.Page
	)

	for self.DataTableNumRows < num && err == nil {
		page, err = self.ReadPageForSkip()
	}
	if num > self.DataTableNumRows {
		num = self.DataTableNumRows
	}
	if page != nil {
		if err = page.GetValueFromRawData(self.SchemaHandler); err != nil {
			return 0
		}
		page.Decode(self.DictPage)
		i, j := len(self.DataTable.Values)-1, len(page.DataTable.Values)-1
		for i >= 0 && j >= 0 {
			self.DataTable.Values[i] = page.DataTable.Values[j]
			i, j = i-1, j-1
		}
	}

	self.DataTable.Pop(num)
	self.DataTableNumRows -= num
	if self.DataTableNumRows <= 0 {
		tmp := self.DataTable
		self.DataTable = Layout.NewTableFromTable(tmp)
		self.DataTable.Merge(tmp)
	}
	return num
}

func (self *ColumnBufferType) ReadRows(num int64) (*Layout.Table, int64) {
	var err error

	for self.DataTableNumRows < num && err == nil {
		err = self.ReadPage()
	}

	if self.DataTableNumRows < 0 {
		self.DataTableNumRows = 0
		self.DataTable = Layout.NewEmptyTable()
	}

	if num > self.DataTableNumRows {
		num = self.DataTableNumRows
	}

	res := self.DataTable.Pop(num)
	self.DataTableNumRows -= num

	if self.DataTableNumRows <= 0 { //release previous slice memory
		tmp := self.DataTable
		self.DataTable = Layout.NewTableFromTable(tmp)
		self.DataTable.Merge(tmp)
	}
	return res, num

}
