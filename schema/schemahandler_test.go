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


func TestNewSchemaHandlerFromSchemaList(t *testing.T) {

	//get any schema
	sourceSchema, err := NewSchemaHandlerFromStruct(new(Student))

	assert.NoError(t, err)
	if err != nil {
		panic(err)
	}

	schema := NewSchemaHandlerFromSchemaList(sourceSchema.SchemaElements)

	for path, idx := range schema.MapIndex {
		assert.Equal(t, *schema.SchemaElements[idx].RepetitionType, schema.Infos[idx].RepetitionType, "RepetitionType of 'SchemaElements' does not match RepetitionType of 'Infos' for Element at path %s (pos %d)", path, idx)
	}

}
