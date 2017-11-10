package SchemaHandler

import (
	"fmt"
	"github.com/xitongsys/parquet-go/ParquetType"
	"testing"
)

type Class struct {
	Name   ParquetType.UTF8
	Number ParquetType.INT32
	Score  ParquetType.DECIMAL `Scale:"3" Precision:"10" "BaseType":INT32`
}

type Student struct {
	Name    ParquetType.UTF8
	Age     ParquetType.INT64
	Classes []*Class
	Info    *map[ParquetType.UTF8]*ParquetType.UTF8
	Sex     *ParquetType.BOOLEAN
}

func TestNewSchemaHandlerFromStruct(t *testing.T) {
	schemaMap := NewSchemaHandlerFromStruct(new(Student))
	fmt.Println(schemaMap)
}
