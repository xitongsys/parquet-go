package parquet_go

import (
	"fmt"
	"testing"
)

type Class struct {
	Name   string
	Number int32
	Score  float32
}

type Student struct {
	Name    string
	Age     int32
	Classes []Class
}

func TestNewSchemaHandlerFromStruct(t *testing.T) {
	schemaMap := NewSchemaHandlerFromStruct(new(Student))
	fmt.Println(schemaMap)
}
