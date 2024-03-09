package schema

import (
	"reflect"
	"strings"
	"testing"

	"github.com/AppliedIntuition/parquet-go/common"
	"github.com/AppliedIntuition/parquet-go/parquet"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type JobStatus int32

const (
	JobStatus_UNSPECIFIED            JobStatus = 0
	JobStatus_BLOCKED                JobStatus = 1
	JobStatus_ENQUEUED               JobStatus = 2
	JobStatus_RUNNING                JobStatus = 3
	JobStatus_COMPLETED              JobStatus = 4
	JobStatus_ERRORED                JobStatus = 5
	JobStatus_CANCELLED              JobStatus = 6
	JobStatus_UPSTREAM_NOT_PROCESSED JobStatus = 7
)

func (x JobStatus) Enum() *JobStatus {
	p := new(JobStatus)
	*p = x
	return p
}

type ClassNoTag struct {
	Name   string
	Number int
	Score  string
}

type StudentNoTag struct {
	Name    string
	Age     int
	Classes []*ClassNoTag
	Info    *map[string]*string
	Sex     *bool
}

type ProtoMessage struct {
	Timestamp timestamppb.Timestamp
	Status    JobStatus
	IntVal    int32
	Bytes     []byte
	String    string
}

func TestProtoSpecificSchema(t *testing.T) {
	schemaHandler, err := NewSchemaHandlerFromStruct(new(ProtoMessage), false)
	if err != nil {
		t.Errorf("failed to generate schema handler: %v", err)
	}
	assert.Equal(t, 5, len(schemaHandler.ValueColumns))
	assert.Equal(t, parquet.Type_INT64, *schemaHandler.SchemaElements[1].Type)
	assert.Equal(t, parquet.ConvertedType_TIMESTAMP_MILLIS, *schemaHandler.SchemaElements[1].ConvertedType)
	assert.Equal(t, parquet.Type_BYTE_ARRAY, *schemaHandler.SchemaElements[2].Type)
	assert.Equal(t, parquet.ConvertedType_ENUM, *schemaHandler.SchemaElements[2].ConvertedType)
	assert.Equal(t, parquet.Type_INT32, *schemaHandler.SchemaElements[3].Type)
	assert.Nil(t, schemaHandler.SchemaElements[3].ConvertedType)
	assert.Equal(t, parquet.Type_BYTE_ARRAY, *schemaHandler.SchemaElements[4].Type)
	assert.Nil(t, schemaHandler.SchemaElements[4].ConvertedType)
	assert.Nil(t, schemaHandler.SchemaElements[4].LogicalType)
	assert.Equal(t, parquet.Type_BYTE_ARRAY, *schemaHandler.SchemaElements[5].Type)
	assert.Nil(t, schemaHandler.SchemaElements[5].ConvertedType)
	assert.NotNil(t, schemaHandler.SchemaElements[5].LogicalType.STRING)
}

func TestNewSchemaHandlerFromProtStruct(t *testing.T) {
	schemaHandler, err := NewSchemaHandlerFromStruct(new(StudentNoTag), false)
	if err != nil {
		t.Errorf("failed to generate schema handler: %v", err)
	}
	assert.Equal(t, 14, len(schemaHandler.SchemaElements))
	assert.Equal(t, 8, len(schemaHandler.ValueColumns))
	expectedValues := []string{
		"Parquet_go_root\x01Name",
		"Parquet_go_root\x01Age",
		"Parquet_go_root\x01Classes\x01List\x01Element\x01Name",
		"Parquet_go_root\x01Classes\x01List\x01Element\x01Number",
		"Parquet_go_root\x01Classes\x01List\x01Element\x01Score",
		"Parquet_go_root\x01Info\x01Key_value\x01Key",
		"Parquet_go_root\x01Info\x01Key_value\x01Value",
		"Parquet_go_root\x01Sex",
	}
	assert.ElementsMatch(t, expectedValues, schemaHandler.ValueColumns)
	structure := `Parquet_go_root
  Info: Parquet_go_rootInfo
    Key_value: Parquet_go_rootInfoKey_value
      Key: Parquet_go_rootInfoKey_valueKey
      Value: Parquet_go_rootInfoKey_valueValue
  Sex: Parquet_go_rootSex
  Name: Parquet_go_rootName
  Age: Parquet_go_rootAge
  Classes: Parquet_go_rootClasses
    List: Parquet_go_rootClassesList
      Element: Parquet_go_rootClassesListElement
        Name: Parquet_go_rootClassesListElementName
        Number: Parquet_go_rootClassesListElementNumber
        Score: Parquet_go_rootClassesListElementScore
`

	var builder strings.Builder
	builder = *schemaHandler.PathMap.output(&builder, "")
	assert.ElementsMatch(t, strings.Split(structure, "\n"), strings.Split(builder.String(), "\n"))
}

func TestTagGeneration(t *testing.T) {
	expected := make(map[string]*common.Tag)
	expected["Sex"], _ = common.StringToTag("type=BOOLEAN, repetitiontype=OPTIONAL, name=Sex")
	expected["Info"], _ = common.StringToTag("type=MAP, keytype=BYTE_ARRAY, valuetype=BYTE_ARRAY, repetitiontype=OPTIONAL, name=Info")
	expected["Classes"], _ = common.StringToTag("type=LIST, name=Classes")
	expected["Age"], _ = common.StringToTag("type=INT64, name=Age")
	expected["Name"], _ = common.StringToTag("type=BYTE_ARRAY, name=Name")

	actual := make(map[string]*common.Tag)
	tp := reflect.TypeOf(StudentNoTag{})
	tv := reflect.ValueOf(StudentNoTag{})
	for i := tp.NumField() - 1; i >= 0; i-- {
		tagString, err := GenerateFieldTag(tp.Field(i), tv.Field(i))
		if err != nil {
			t.Errorf("failed to generate tag: %v", err)
		}
		actual[tp.Field(i).Name], _ = common.StringToTag(tagString)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("The tag is different")
	}
}
