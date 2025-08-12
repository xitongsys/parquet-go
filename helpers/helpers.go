package helpers

import (
	"reflect"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var timeType = reflect.TypeOf(time.Time{})
var timeTypePtr = reflect.TypeOf(&time.Time{})
var timestampType = reflect.TypeOf(timestamppb.Timestamp{})
var timestampTypePtr = reflect.TypeOf(&timestamppb.Timestamp{})

func IsTimeStruct(t reflect.Type) bool {
	return t == timeType || t == timestampType || t == timeTypePtr || t == timestampTypePtr
}

func IsTime(t reflect.Type) bool {
	return t == timeType || t == timeTypePtr
}

func IsTimestampPB(t reflect.Type) bool {
	return t == timestampType || t == timestampTypePtr
}
