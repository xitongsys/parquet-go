package writer

import (
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/types"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/parquet"
)

type CSVWriter struct {
	ParquetWriter
}

//Create CSV writer
func NewCSVWriter(md []string, pfile source.ParquetFile, np int64) (*CSVWriter, error) {
	res := new(CSVWriter)
	res.SchemaHandler = schema.NewSchemaHandlerFromMetadata(md)
	res.PFile = pfile
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	res.CompressionType = parquet.CompressionCodec_SNAPPY
	res.PagesMapBuf = make(map[string][]*layout.Page)
	res.DictRecs = make(map[string]*layout.DictRecType)
	res.NP = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.Offset = 4
	_, err := res.PFile.Write([]byte("PAR1"))
	res.MarshalFunc = marshal.MarshalCSV
	return res, err
}

//Write string values to parquet file
func (self *CSVWriter) WriteString(recsi interface{}) error {
	recs := recsi.([]*string)
	lr := len(recs)
	rec := make([]interface{}, lr)
	for i := 0; i < lr; i++ {
		rec[i] = nil
		if recs[i] != nil {
			rec[i] = types.StrToParquetType(*recs[i],
				self.SchemaHandler.SchemaElements[i+1].Type,
				self.SchemaHandler.SchemaElements[i+1].ConvertedType,
				int(self.SchemaHandler.SchemaElements[i+1].GetTypeLength()),
				int(self.SchemaHandler.SchemaElements[i+1].GetScale()),
			)
		}
	}

	return self.Write(rec)
}
