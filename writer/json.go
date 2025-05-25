package writer

import (
	"io"

	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/marshal"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
)

type JSONWriter struct {
	ParquetWriter
}

func NewJSONWriterFromWriter(jsonSchema string, w io.Writer, np int64) (*JSONWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewJSONWriter(jsonSchema, wf, np)
}

// Create JSON writer
func NewJSONWriter(jsonSchema string, pfile source.ParquetFileWriter, np int64) (*JSONWriter, error) {
	var err error
	res := new(JSONWriter)
	res.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema)
	if err != nil {
		return res, err
	}

	res.PFile = pfile
	res.PageSize = 8 * 1024              // 8K
	res.RowGroupSize = 128 * 1024 * 1024 // 128M
	res.CompressionType = parquet.CompressionCodec_SNAPPY
	res.PagesMapBuf = make(map[string][]*layout.Page)
	res.DictRecs = make(map[string]*layout.DictRecType)
	res.NP = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.Offset = 4
	_, err = res.PFile.Write([]byte("PAR1"))
	res.MarshalFunc = marshal.MarshalJSON
	return res, err
}
