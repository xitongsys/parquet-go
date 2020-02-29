package schema

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type Class struct {
	Name   string `parquet:"name=name, type=UTF8"`
	Number int    `parquet:"name=number, type=INT32"`
	Score  string `parquet:"scale=3, precision=10, type=INT32"`
}

type Student struct {
	Name    string              `parquet:"name=name, type=UTF8"`
	Age     int                 `parquet:"name=number, type=INT64"`
	Classes []*Class            `parquet:"name=classes"`
	Info    *map[string]*string `parquet:"name=info, type=UTF8, keytype=UTF8"`
	Sex     *bool               `parquet:"name=sex, type=BOOLEAN"`
}

func TestNewSchemaHandlerFromStruct(t *testing.T) {
	schemaMap, _ := NewSchemaHandlerFromStruct(new(Student))
	fmt.Println(schemaMap)
}


func TestValueColumnsSchemaElementsOfNewSchemaHandlerFromSchemaList(t *testing.T) {

	t.Skip("Obsolete")

	//get any schema
	sourceSchema, err := NewSchemaHandlerFromStruct(new(Student))

	assert.NoError(t, err)
	if err != nil {
		panic(err)
	}

	schema := NewSchemaHandlerFromSchemaList(sourceSchema.SchemaElements)
	assert.EqualValues(t, sourceSchema.SchemaElements, schema.SchemaElements)

	for _, path := range schema.ValueColumns {

		colIdx := schema.MapIndex[path]

		expectedElement := sourceSchema.SchemaElements[colIdx]
		sourceElement := schema.SchemaElements[colIdx]


		assert.EqualValues(t, *expectedElement, *sourceElement, "initial 'SchemaElement' of schema created by struct does not match 'SchemaElement' created by list for Element at path %s (pos %d)", path, colIdx)
	}
}
