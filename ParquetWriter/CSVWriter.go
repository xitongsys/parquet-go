package ParquetWriter

import (
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/Marshal"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

type CSVWriter struct {
	ParquetWriter
}

//Create CSV writer
func NewCSVWriter(md []string, pfile ParquetFile.ParquetFile, np int64) (*CSVWriter, error) {
	res := new(CSVWriter)
	res.SchemaHandler = SchemaHandler.NewSchemaHandlerFromMetadata(md)
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
	_, err := res.PFile.Write([]byte("PAR1"))
	res.MarshalFunc = Marshal.MarshalCSV
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
			rec[i] = ParquetType.StrToParquetType(*recs[i],
				self.SchemaHandler.SchemaElements[i+1].Type,
				self.SchemaHandler.SchemaElements[i+1].ConvertedType,
				int(self.SchemaHandler.SchemaElements[i+1].GetTypeLength()),
				int(self.SchemaHandler.SchemaElements[i+1].GetScale()),
			)
		}
	}

	return self.Write(rec)
}
