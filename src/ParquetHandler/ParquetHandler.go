package ParquetHandler

import (
	. "Layout"
	. "SchemaHandler"
	"parquet"
)

type ParquetFile interface {
	Seek(offset int, pos int)
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close()
	Open(name string) error
	Create(name string) error
}

type ParquetHandler struct {
	SchemaHandler *SchemaHandler
	NP            int64 //parallel number

	Footer    *parquet.FileMetaData
	RowGroups []*RowGroup

	PFile ParquetFile

	///read info///////
	RowGroupIndex int64

	////write info/////
	PageSize     int64
	RowGroupSize int64
	Offset       int64
	Objs         []interface{}
	Size         int64
}

func NewParquetHandler() *ParquetHandler {
	res := new(ParquetHandler)
	res.NP = 1
	res.FileMap = make(map[string]*ParquetFile)
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 256 * 1024 * 1024 //256M
	return res
}
