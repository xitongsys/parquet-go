package writer

import (
	"fmt"

	"github.com/AppliedIntuition/parquet-go/marshal"
	"github.com/AppliedIntuition/parquet-go/schema"
	"github.com/xitongsys/parquet-go/source"
)

// Pass the obj as the go struct object
func NewParquetWriterFromProto(pFile source.ParquetFile, obj interface{}, np int64) (*ParquetWriter, error) {
	schemaHandler, err := schema.NewSchemaHandlerFromProtoStruct(obj, false)
	if err != nil {
		return nil, fmt.Errorf("failed to generate schema handler: %v", err)
	}
	parquetWriter, err := NewParquetWriter(pFile, schemaHandler, np)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer %v", err)
	}
	parquetWriter.MarshalFunc = marshal.MarshalProto
	return parquetWriter, nil
}
