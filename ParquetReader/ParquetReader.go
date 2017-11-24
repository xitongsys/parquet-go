package ParquetHandler

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
)

//Convert a file reater to Thrift reader
func ConvertToThriftReader(file ParquetFile, offset int64, size int64) *thrift.TBufferedTransport {
	file.Seek(int(offset), 0)
	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(size))
	return bufferReader
}

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

func (self *ColumnBufferType) ReadRows(dst interface{}) *Common.Table {
	var err error
	num := reflect.ValueOf(dst).Elem().Len()
	for self.DataTableNumRows < num && err == nil {
		err = self.ReadPage()
	}
	if num > self.DataTableNumRows {
		num = self.DataTableNumRows
	}

}

type ParquetReader struct {
	SchemaHandler *SchemaHandler.SchemaHandler
	NP            int64 //parallel number
	Footer        *parquet.FileMetaData
	PFile         ParquetFile

	RowGroupIndex int64
	TotalNumRows  int64
	ChunkRecs     []*ChunkRecType

	TableMap map[string]*Table
	NumRows  map[string]int64
}

//Create a parquet handler
func NewParquetReader() *ParquetReader {
	res := new(ParquetReader)
	res.NP = 1
	return res
}

//Get the footer size
func (self *ParquetReader) GetFooterSize() uint32 {
	buf := make([]byte, 4)
	self.PFile.Seek(-8, 2)
	self.PFile.Read(buf)
	size := binary.LittleEndian.Uint32(buf)
	return size
}

//Read footer from parquet file
func (self *ParquetReader) ReadFooter() {
	size := self.GetFooterSize()
	self.PFile.Seek(int(-(int64)(8+size)), 2)
	self.Footer = parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactory()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(self.PFile))
	self.Footer.Read(protocol)
}

// init function. np is the parallel number
func (self *ParquetReader) ReadInit(pfile ParquetFile, np int64) {
	self.PFile = pfile
	self.NP = np
	self.ReadFooter()
	self.SchemaHandler = SchemaHandler.NewSchemaHandlerFromSchemaList(self.Footer.GetSchema())
	self.RowGroupIndex = 0
	return len(self.Footer.GetRowGroups())
}

func (self *ParquetReader) ReadRowGroup() int64 {
	rowGroups := self.Footer.GetRowGroups()
	ln := int64(len(rowGroups))
	if self.RowGroupIndex >= ln {
		return 0
	}
	rowGroupHeader := rowGroups[self.RowGroupIndex]
	self.RowGroupIndex++

	columnChunks := rowGroupHeader.GetColumns()
	ln = len(columnChunks)
	self.ChunkRecs = make([]*ChunkRecType, ln)
	for i := 0; i < ln; i++ {
		offset := columnChunks[i].FileOffset
		PFile := self.PFile
		if columnChunks[i].FilePath != nil {
			PFile, _ = self.PFile.Open(*columnChunks[i].FilePath)
		} else {
			PFile, _ = self.PFile.Open("")
		}
		size := columnChunks[i].MetaData.GetTotalCompressedSize()

		self.ChunkRecs[i] = &ChunkRecType{
			ThriftReader: ConvertToThriftReader(PFile, offset, size),
			ChunkHeader:  columnChunks[i],
			ReadValues:   0,
		}
	}
	self.TotalNumRows = rowGroupHeader.GetNumRows

}

func (self *ParquetReader) Read(interface{}) {

}
