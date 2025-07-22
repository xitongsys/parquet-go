package marshal

import (
	"reflect"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func isTimestamp(t reflect.Type) bool {
	switch t {
	case reflect.TypeOf(time.Time{}):
		return true
	case reflect.TypeOf(&time.Time{}):
		return true
	case reflect.TypeOf(&timestamppb.Timestamp{}):
		return true
	case reflect.TypeOf(timestamppb.Timestamp{}):
		return true
	default:
		return false
	}
}

func convertToUnixTimestamp(v interface{}) int64 {
	switch val := v.(type) {
	case time.Time:
		return val.UnixMilli()
	case *time.Time:
		if val == nil {
			return 0
		}
		return val.UnixMilli()
	case *timestamppb.Timestamp:
		if val == nil {
			return 0
		}
		return val.AsTime().UnixMilli()
	case timestamppb.Timestamp:
		return val.AsTime().UnixMilli()
	default:
		// Optional: handle reflect.Value case
		if rv, ok := v.(reflect.Value); ok && rv.IsValid() && !rv.IsZero() {
			return convertToUnixTimestamp(rv.Interface())
		}
		return 0
	}
}
