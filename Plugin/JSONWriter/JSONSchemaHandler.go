package JSONWriter

import (
	"encoding/json"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"log"
)

type SchemaItemType struct {
	Info   map[string]interface{}
	Fields []*SchemaItemType
}

func NewSchemaItem() *SchemaItemType {
	res := new(SchemaItemType)
	res.Info = make(map[stirng]interface{})
	return res
}

func NewSchemaHandlerFromJSON(str string) *SchemaHandler.SchemaHandler {
	schema := NewSchemaItem()
	json.Unmarshal([]byte(str), schema)

}
