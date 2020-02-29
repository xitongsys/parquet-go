package writer

import (
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/schema"
	"testing"
)

func TestWritingParquetWithSchemaCreatedFromSchemaElements(t *testing.T) {


	type localWritingTestType struct {
		StringProperty       string `parquet:"name=stringProperty, type=UTF8"`
		IntProperty          int64    `parquet:"name=intProperty, type=INT64"`
		OptionalBoolProperty *bool  `parquet:"name=optionalBoolProperty, type=BOOLEAN"`
	}

	//get any schema
	sourceSchema, err := schema.NewSchemaHandlerFromStruct(new(localWritingTestType))

	assert.NoError(t, err)
	if err != nil {
		panic(err)
	}

	schema := schema.NewSchemaHandlerFromSchemaList(sourceSchema.SchemaElements)

	var byteBuffer []byte

	outFile, err := buffer.NewBufferFile(byteBuffer)
	assert.NoError(t, err)
	if err != nil {
		panic(err)
	}

	wr, err := NewParquetWriter(outFile, nil, 1)
	assert.NoError(t, err)
	if err != nil {
		panic(err)
	}

	//cloning the behaviour on inserting the schema (taken from "SetSchemaHandlerFromJSON")
	wr.SchemaHandler = schema
	wr.Footer.Schema = wr.Footer.Schema[:0]
	wr.Footer.Schema = append(wr.Footer.Schema, wr.SchemaHandler.SchemaElements...)

	//writing....
	trueValue := true
	err = wr.Write(localWritingTestType{
		StringProperty:       "String",
		IntProperty:          int64(1),
		OptionalBoolProperty: &trueValue,
	})
	assert.NoError(t, err)
	if err != nil {
		panic(err)
	}

	err = wr.WriteStop()
	assert.NoError(t, err)
	if err != nil {
		panic(err)
	}

	err = outFile.Close()
	assert.NoError(t, err)
	if err != nil {
		panic(err)
	}

}
