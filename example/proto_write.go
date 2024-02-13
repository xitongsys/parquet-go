package main

import (
	"fmt"
	"log"

	"github.com/AppliedIntuition/parquet-go/writer"
	"github.com/xitongsys/parquet-go-source/local"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
		0: "ASYNCJOBSTATUS_UNSPECIFIED",
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
type TestInterface interface {
	foo()
}

type TestInterfaceImpl1 struct {
	Bar string
}

type TestInterfaceImpl2 struct {
	Test            string
	NestedInterface TestInterface
}

func (t *TestInterfaceImpl1) foo() {
	fmt.Println(t.Bar)
}

func (t *TestInterfaceImpl2) foo() {
	fmt.Print(t.Test)
}

type TestInterfaceStruct struct {
	Val       TestInterface
	NestedVal TestInterface
	Arr       [][]TestInterface
	Message   ProtoMessage
	UintVal   uint
	UintVal32 uint32
	UintVal64 uint64
}

func main() {
	protoMessages := []ProtoMessage{
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 1, Nanos: 1000000}, Status: JobStatus_RUNNING, IntVal: 1},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 2, Nanos: 1000000}, Status: JobStatus_ENQUEUED, IntVal: 2},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 3, Nanos: 1000000}, Status: JobStatus_COMPLETED, IntVal: 3},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 4, Nanos: 1000000}, Status: JobStatus_ERRORED, IntVal: 4},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 5, Nanos: 1000000}, Status: JobStatus_CANCELLED, IntVal: 5},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 6, Nanos: 1000000}, Status: JobStatus_UPSTREAM_NOT_PROCESSED, IntVal: 6},
	}
	impl2 := TestInterfaceImpl2{
		Test:            "test",
		NestedInterface: &TestInterfaceImpl1{Bar: "bar1"},
	}
	impl1 := TestInterfaceImpl1{Bar: "bar2"}

	vals := make([]TestInterfaceStruct, 6)
	for index, message := range protoMessages {
		vals[index] = TestInterfaceStruct{
			Val:       &impl1,
			NestedVal: &impl2,
			Arr:       [][]TestInterface{{&impl2, &impl2}, {&impl2}},
			Message:   message,
		}
	}

	fw, err := local.NewLocalFileWriter("output/proto_message.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := writer.NewParquetWriterFromProto(fw, &vals[0], 1)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	for _, val := range vals {
		if err = pw.Write(val); err != nil {
			log.Println("Write error", err)
			return
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	fw.Close()
	log.Println("Write Finished")

}
