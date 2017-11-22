package ParquetHandler

import (
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

type ParquetFile interface {
	Seek(offset int, pos int) (int64, error)
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close()
	Open(name string) (ParquetFile, error)
	Create(name string) (ParquetFile, error)
}

//ParquetHandler is a handler for read/write parquet file
type ParquetHandler struct {
	SchemaHandler *SchemaHandler.SchemaHandler
	NP            int64 //parallel number

	Footer    *parquet.FileMetaData
	RowGroups []*Layout.RowGroup

	PFile ParquetFile

	///read info///////
	RowGroupIndex int64

	////write info/////
	PageSize     int64
	RowGroupSize int64
	Offset       int64

	Objs              []interface{}
	ObjsSize          int64
	ObjSize           int64
	CheckSizeCritical int64

	PagesMapBuf map[string][]*Layout.Page
	Size        int64
}

//Create a parquet handler
func NewParquetHandler() *ParquetHandler {
	res := new(ParquetHandler)
	res.NP = 1
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	res.ObjsSize = 0
	res.CheckSizeCritical = 0
	res.Size = 0
	res.PagesMapBuf = make(map[string][]*Layout.Page)
	return res
}
