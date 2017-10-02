package ParquetHandler

import (
	. "github.com/xitongsys/parquet-go/Layout"
	. "github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

type ParquetFile interface {
	Seek(offset int, pos int) (int64, error)
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
	ObjAveSize   int64

	Size int64
}

func NewParquetHandler() *ParquetHandler {
	res := new(ParquetHandler)
	res.NP = 1
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //256M
	return res
}
