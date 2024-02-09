package main

import (
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

func main() {
	protoMessages := []interface{}{
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 1, Nanos: 1000000}, Status: JobStatus_RUNNING, IntVal: 1},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 2, Nanos: 1000000}, Status: JobStatus_ENQUEUED, IntVal: 2},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 3, Nanos: 1000000}, Status: JobStatus_COMPLETED, IntVal: 3},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 4, Nanos: 1000000}, Status: JobStatus_ERRORED, IntVal: 4},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 5, Nanos: 1000000}, Status: JobStatus_CANCELLED, IntVal: 5},
		ProtoMessage{Timestamp: timestamppb.Timestamp{Seconds: 6, Nanos: 1000000}, Status: JobStatus_UPSTREAM_NOT_PROCESSED, IntVal: 6},
	}

	fw, err := local.NewLocalFileWriter("output/proto_message.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := writer.NewParquetWriterFromProto(fw, new(ProtoMessage), 1)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	for _, message := range protoMessages {
		if err = pw.Write(message); err != nil {
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
