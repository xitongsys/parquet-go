package schema

import (
	"fmt"
	"testing"
)

type Class struct {
	Name   string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Number int    `parquet:"name=number, type=INT32"`
	Score  string `parquet:"scale=3, precision=10, type=INT32"`
}

type Student struct {
	Name    string              `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age     int                 `parquet:"name=number, type=INT64"`
	Classes []*Class            `parquet:"name=classes"`
	Info    *map[string]*string `parquet:"name=info, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Sex     *bool               `parquet:"name=sex, type=BOOLEAN"`
}

func TestNewSchemaHandlerFromStruct(t *testing.T) {
	schemaMap, err := NewSchemaHandlerFromStruct(new(Student))
	if err != nil {
		t.Errorf("error in creating schema from struct :%v", err.Error())
	}
	fmt.Println(schemaMap)
}
