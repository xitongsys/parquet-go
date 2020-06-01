package schema

import "testing"

func TestNewSchemaHandlerFromJSON(t *testing.T) {
	var jsonSchema string = `
	{
	  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
	  "Fields": [
		{"Tag": "name=name, inname=Name, type=UTF8, repetitiontype=REQUIRED"},
		{"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"}
	  ]
	}
	`
	handler, err := NewSchemaHandlerFromJSON(jsonSchema)
	if err != nil {
		t.Errorf("error in creating handler from json schema :%v", err.Error())
	}

	expectedElems := 1 + 2 //goroot +2
	if len(handler.SchemaElements) != expectedElems {
		t.Errorf("expected %v elements from json schema string, got %v", expectedElems, len(handler.SchemaElements))
	}
}

func TestNewSchemaHandlerFromImproperJSON(t *testing.T) {
	var improperJsonSchema string = `
	{
	  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
	  "Fields": [
		{"Tag": "name=name, inname=Name, type=UTF8, repetitiontype=REQUIRED"},
		{"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"}
		,,
	  ]
	}
	`
	_, err := NewSchemaHandlerFromJSON(improperJsonSchema)
	if err == nil {
		t.Errorf("failing test, expected error as we provided an improperly formatted json string, but got no error!")
	}

}
