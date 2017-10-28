package ParquetHandler

import (
	. "github.com/xitongsys/parquet-go/Layout"
	. "github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"strings"
)

type ParquetFile interface {
	Seek(offset int, pos int) (int64, error)
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close()
	Open(name string) (ParquetFile, error)
	Create(name string) (ParquetFile, error)
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
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	return res
}

func (self *ParquetHandler) NameToLower() {
	for _, schema := range self.Footer.Schema {
		schema.Name = strings.ToLower(schema.Name)
	}
	for _, rowGroup := range self.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			ln := len(chunk.MetaData.PathInSchema)
			for i := 0; i < ln; i++ {
				chunk.MetaData.PathInSchema[i] = strings.ToLower(chunk.MetaData.PathInSchema[i])
			}
		}
	}
}
