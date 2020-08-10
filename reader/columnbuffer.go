package reader

import (
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/syucream/parquet-go/common"
	"github.com/syucream/parquet-go/layout"
	"github.com/syucream/parquet-go/parquet"
	"github.com/syucream/parquet-go/schema"
)

type ColumnBufferType struct {
	r ReadSeekCloser

	ThriftReader *thrift.TBufferedTransport

	Footer        *parquet.FileMetaData
	SchemaHandler *schema.SchemaHandler

	PathStr       string
	RowGroupIndex int64
	ChunkHeader   *parquet.ColumnChunk

	ChunkReadValues int64

	DictPage *layout.Page

	DataTable        *layout.Table
	DataTableNumRows int64
}

func NewColumnBuffer(r ReadSeekCloser, footer *parquet.FileMetaData, schemaHandler *schema.SchemaHandler, pathStr string) (*ColumnBufferType, error) {
	res := &ColumnBufferType{
		r:                r,
		Footer:           footer,
		SchemaHandler:    schemaHandler,
		PathStr:          pathStr,
		DataTableNumRows: -1,
	}

	if err := res.NextRowGroup(); err != io.EOF {
		return nil, err
	}

	return res, nil
}

func (self *ColumnBufferType) NextRowGroup() error {
	rowGroups := self.Footer.GetRowGroups()
	ln := int64(len(rowGroups))
	if self.RowGroupIndex >= ln {
		self.DataTableNumRows++ //very important, because DataTableNumRows is one smaller than real rows number
		return io.EOF
	}

	self.RowGroupIndex++

	columnChunks := rowGroups[self.RowGroupIndex-1].GetColumns()
	i := int64(0)
	ln = int64(len(columnChunks))
	for i = 0; i < ln; i++ {
		path := make([]string, 0)
		path = append(path, self.SchemaHandler.GetRootInName())
		path = append(path, columnChunks[i].MetaData.GetPathInSchema()...)

		if self.PathStr == common.PathToStr(path) {
			break
		}
	}

	if i >= ln {
		return fmt.Errorf("[NextRowGroup] Column not found: %v", self.PathStr)
	}

	self.ChunkHeader = columnChunks[i]
	if columnChunks[i].FilePath != nil {
		if err := self.r.Close(); err != nil {
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
		if err := self.ThriftReader.Close(); err != nil {
			return err
		}
	}

	self.ThriftReader = ConvertToThriftReader(self.r, offset, size)
	self.ChunkReadValues = 0
	self.DictPage = nil
	return nil
}

func (self *ColumnBufferType) ReadPage() error {
	if self.ChunkHeader != nil && self.ChunkHeader.MetaData != nil && self.ChunkReadValues < self.ChunkHeader.MetaData.NumValues {
		page, numValues, numRows, err := layout.ReadPage(self.ThriftReader, self.SchemaHandler, self.ChunkHeader.MetaData)
		if err != nil {
			//data is nil and rl/dl=0, no pages in file
			if err == io.EOF {
				if self.DataTable == nil {
					index := self.SchemaHandler.MapIndex[self.PathStr]
					self.DataTable = layout.NewEmptyTable()
					self.DataTable.Schema = self.SchemaHandler.SchemaElements[index]
					self.DataTable.Path = common.StrToPath(self.PathStr)

				}

				self.DataTableNumRows = self.ChunkHeader.MetaData.NumValues

				for self.ChunkReadValues < self.ChunkHeader.MetaData.NumValues {
					self.DataTable.Values = append(self.DataTable.Values, nil)
					self.DataTable.RepetitionLevels = append(self.DataTable.RepetitionLevels, int32(0))
					self.DataTable.DefinitionLevels = append(self.DataTable.DefinitionLevels, int32(0))
					self.ChunkReadValues++
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
			self.DataTable = layout.NewTableFromTable(page.DataTable)
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

func (self *ColumnBufferType) ReadPageForSkip() (*layout.Page, error) {
	if self.ChunkHeader != nil && self.ChunkHeader.MetaData != nil && self.ChunkReadValues < self.ChunkHeader.MetaData.NumValues {
		page, err := layout.ReadPageRawData(self.ThriftReader, self.SchemaHandler, self.ChunkHeader.MetaData)
		if err != nil {
			return nil, err
		}

		numValues, numRows, err := page.GetRLDLFromRawData(self.SchemaHandler)
		if err != nil {
			return nil, err
		}

		if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
			err = page.GetValueFromRawData(self.SchemaHandler)
			if err != nil {
				return nil, err
			}
			self.DictPage = page
			return page, nil
		}

		if self.DataTable == nil {
			self.DataTable = layout.NewTableFromTable(page.DataTable)
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
		page *layout.Page
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
		self.DataTable = layout.NewTableFromTable(tmp)
		self.DataTable.Merge(tmp)
	}

	return num
}

func (self *ColumnBufferType) ReadRows(num int64) (*layout.Table, int64) {
	var err error

	for self.DataTableNumRows < num && err == nil {
		err = self.ReadPage()
	}

	if self.DataTableNumRows < 0 {
		self.DataTableNumRows = 0
		self.DataTable = layout.NewEmptyTable()
	}

	if num > self.DataTableNumRows {
		num = self.DataTableNumRows
	}

	res := self.DataTable.Pop(num)
	self.DataTableNumRows -= num

	if self.DataTableNumRows <= 0 { //release previous slice memory
		tmp := self.DataTable
		self.DataTable = layout.NewTableFromTable(tmp)
		self.DataTable.Merge(tmp)
	}
	return res, num

}

//Convert a file reater to Thrift reader
func ConvertToThriftReader(file io.ReadSeeker, offset int64, size int64) *thrift.TBufferedTransport {
	// TODO check errors
	_, _ = file.Seek(offset, 0)
	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(size))
	return bufferReader
}
