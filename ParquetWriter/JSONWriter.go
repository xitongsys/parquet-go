package ParquetWriter

import (
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/Marshal"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

type JSONWriter struct {
	ParquetWriter
}

//Create JSON writer
func NewJSONWriter(jsonSchema string, pfile ParquetFile.ParquetFile, np int64) (*JSONWriter, error) {
	var err error
	res := new(JSONWriter)
	res.SchemaHandler, err = SchemaHandler.NewSchemaHandlerFromJSON(jsonSchema)
	if err != nil {
		return res, err
	}

	res.PFile = pfile
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	res.CompressionType = parquet.CompressionCodec_SNAPPY
	res.PagesMapBuf = make(map[string][]*Layout.Page)
	res.DictRecs = make(map[string]*Layout.DictRecType)
	res.NP = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.Offset = 4
	_, err = res.PFile.Write([]byte("PAR1"))
	res.MarshalFunc = Marshal.MarshalJSON
	return res, err
}
