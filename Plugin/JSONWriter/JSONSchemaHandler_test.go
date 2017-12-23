package JSONWriter

import (
	"testing"
)

func TestNewSchemaHandlerFromJSON(t *testing.T) {
	str := `
{
    "Tag":"name=parquet-go",
    "Fields":[
        {
            "Tag":"name=name, type=UTF8"
        },
        {
            "Tag":"name=age, type=INT32"
        },
        {
            "Tag":"name=Id, type=INT64"
        }
    ]
}
`
	NewSchemaHandlerFromJSON(str)

}
