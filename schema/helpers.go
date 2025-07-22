package schema

import (
	"reflect"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var timeType = reflect.TypeOf(time.Time{})
var timeTypePtr = reflect.TypeOf(&time.Time{})
var timestammpType = reflect.TypeOf(timestamppb.Timestamp{})
var timestammpTypePtr = reflect.TypeOf(&timestamppb.Timestamp{})

func isTimeStruct(t reflect.Type) bool {
	return t == timeType || t == timestammpType || t == timeTypePtr || t == timeTypePtr
}
