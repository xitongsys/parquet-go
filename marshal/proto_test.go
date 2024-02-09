package marshal

import (
	"fmt"
	"testing"
	"time"

	"github.com/AppliedIntuition/parquet-go/schema"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type subElem struct {
	Val string
}

type testNestedElem struct {
	SubElem     subElem
	SubPtr      *subElem
	SubList     []subElem
	SubRepeated []*subElem
	// Marshal ok but not write since it doesn't have corresponding primitive type
	EmptyElem     struct{}
	EmptyPtr      *struct{}
	EmptyList     []struct{}
	EmptyRepeated []*struct{}
}
type JobStatus int32

const (
	JobStatus_JobStatus_UNSPECIFIED  JobStatus = 0
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

func (x JobStatus) String() string {
	statusToName := map[int32]string{
		0: "JOBSTATUS_UNSPECIFIED",
		1: "BLOCKED",
		2: "ENQUEUED",
		3: "RUNNING",
		4: "COMPLETED",
		5: "ERRORED",
		6: "CANCELLED",
		7: "UPSTREAM_NOT_PROCESSED",
	}
	return statusToName[int32(x)]
}

type ProtoMessage struct {
	Timestamp timestamppb.Timestamp
	Status    JobStatus
	IntVal    int32
}

func TestMarsalProtoSpecific(t *testing.T) {
	protoMessages := []interface{}{
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 1, Nanos: int32(time.Millisecond)}, Status: JobStatus_RUNNING, IntVal: 1},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 2, Nanos: int32(time.Millisecond)}, Status: JobStatus_ENQUEUED, IntVal: 2},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 3, Nanos: int32(time.Millisecond)}, Status: JobStatus_COMPLETED, IntVal: 3},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 4, Nanos: int32(time.Millisecond)}, Status: JobStatus_ERRORED, IntVal: 4},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 5, Nanos: int32(time.Millisecond)}, Status: JobStatus_CANCELLED, IntVal: 5},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 6, Nanos: int32(time.Millisecond)}, Status: JobStatus_UPSTREAM_NOT_PROCESSED, IntVal: 6},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 5, Nanos: int32(time.Millisecond)}, Status: JobStatus_BLOCKED, IntVal: 6},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 5, Nanos: int32(time.Millisecond)}, Status: JobStatus_JobStatus_UNSPECIFIED, IntVal: 7},
	}

	schemaHandler, err := schema.NewSchemaHandlerFromProtoStruct(ProtoMessage{})
	if err != nil {
		t.Errorf("failed to get schema handler: %v", err)
	}
	tableMap, err := MarshalProto(protoMessages, schemaHandler)
	if err != nil {
		t.Errorf("failed to marshal values: %v", err)
	}
	const timestampField = "Parquet_go_root\x01Timestamp"
	const statusField = "Parquet_go_root\x01Status"
	assert.Equal(t, 3, len(schemaHandler.ValueColumns))
	assert.Equal(t, 8, len((*tableMap)[timestampField].Values))
	assert.Equal(t, int64(1001), (*tableMap)[timestampField].Values[0].(int64))
	assert.Equal(t, int64(2001), (*tableMap)[timestampField].Values[1].(int64))
	assert.Equal(t, int64(3001), (*tableMap)[timestampField].Values[2].(int64))
	assert.Equal(t, int64(4001), (*tableMap)[timestampField].Values[3].(int64))
	assert.Equal(t, int64(5001), (*tableMap)[timestampField].Values[4].(int64))
	assert.Equal(t, int64(6001), (*tableMap)[timestampField].Values[5].(int64))
	assert.Equal(t, int64(5001), (*tableMap)[timestampField].Values[6].(int64))
	assert.Equal(t, int64(5001), (*tableMap)[timestampField].Values[7].(int64))
	assert.Equal(t, 8, len((*tableMap)[statusField].Values))
	assert.Equal(t, "RUNNING", (*tableMap)[statusField].Values[0].(string))
	assert.Equal(t, "ENQUEUED", (*tableMap)[statusField].Values[1].(string))
	assert.Equal(t, "COMPLETED", (*tableMap)[statusField].Values[2].(string))
	assert.Equal(t, "ERRORED", (*tableMap)[statusField].Values[3].(string))
	assert.Equal(t, "CANCELLED", (*tableMap)[statusField].Values[4].(string))
	assert.Equal(t, "UPSTREAM_NOT_PROCESSED", (*tableMap)[statusField].Values[5].(string))
	assert.Equal(t, "BLOCKED", (*tableMap)[statusField].Values[6].(string))
	assert.Equal(t, "JOBSTATUS_UNSPECIFIED", (*tableMap)[statusField].Values[7].(string))

	fmt.Println(tableMap)
}

func TestMarshalTestNestedElem(t *testing.T) {
	subElemHi := subElem{Val: "hi"}
	subElemThere := subElem{Val: "there"}
	testNestedElems := []interface{}{
		testNestedElem{},
		testNestedElem{SubElem: subElemHi},
		testNestedElem{SubPtr: &subElemHi},
		testNestedElem{SubList: []subElem{}},
		testNestedElem{SubList: []subElem{subElemHi}},
		testNestedElem{SubList: []subElem{subElemHi, {}, subElemThere}},
		testNestedElem{SubRepeated: []*subElem{}},
		testNestedElem{SubRepeated: []*subElem{&subElemHi}},
		testNestedElem{SubRepeated: []*subElem{&subElemHi, nil, &subElemThere}},
		testNestedElem{EmptyPtr: &struct{}{}},
		testNestedElem{EmptyList: []struct{}{}},
		testNestedElem{EmptyList: []struct{}{{}}},
		testNestedElem{EmptyList: []struct{}{{}, {}}},
		testNestedElem{EmptyRepeated: []*struct{}{}},
		testNestedElem{EmptyRepeated: []*struct{}{{}}},
		testNestedElem{EmptyRepeated: []*struct{}{{}, nil, {}}},
	}
	schemaHandler, err := schema.NewSchemaHandlerFromProtoStruct(testNestedElem{})
	if err != nil {
		t.Errorf("failed to get schema handler: %v", err)
	}
	tableMap, err := MarshalProto(testNestedElems, schemaHandler)
	if err != nil {
		t.Errorf("failed to marshal values: %v", err)
	}
	assert.Equal(t, 8, len(schemaHandler.ValueColumns))
	for i := 0; i < 18; i++ {
		assert.Equal(t, nil, (*tableMap)["Parquet_go_root\x01EmptyRepeated\x01List\x01Element"].Values[i])
	}
	const subElemeField = "Parquet_go_root\x01SubElem\x01Val"
	// There are total 16 objects in the list
	assert.Equal(t, 16, len((*tableMap)[subElemeField].Values))
	assert.Equal(t, "hi", (*tableMap)[subElemeField].Values[1].(string))
	// There is one list with 3 values which increase total values to 16+2
	const sublistElemField = "Parquet_go_root\x01SubList\x01List\x01Element\x01Val"
	const repeatedElemField = "Parquet_go_root\x01SubRepeated\x01List\x01Element\x01Val"
	assert.Equal(t, 18, len((*tableMap)[sublistElemField].Values))
	assert.ElementsMatch(t, []int32{0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, (*tableMap)[sublistElemField].DefinitionLevels)
	assert.Equal(t, 18, len((*tableMap)[repeatedElemField].Values))
	assert.ElementsMatch(t, []int32{0, 0, 0, 0, 0, 0, 0, 2, 2, 1, 2, 0, 0, 0, 0, 0, 0, 0}, (*tableMap)[repeatedElemField].DefinitionLevels)
}
