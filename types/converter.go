package types

import (
	"time"
)

func Time2Millis(t time.Time, adjustedToUTC bool) int64 {
	return Time2Micros(t, adjustedToUTC) / 1000
}

func Millis2Time(millis int64, adjustedToUTC bool) time.Time {
	return Micros2Time(millis * 1000, adjustedToUTC)
} 

func Time2Micros(t time.Time, adjustedToUTC bool) int64 {
	return Time2Nanos(t, adjustedToUTC) / 1000
}

func Micros2Time(micros int64, adjustedToUTC bool) time.Time {
	return Nanos2Time(micros * 1000, adjustedToUTC)
}

func Time2Nanos(t time.Time, adjustedToUTC bool) int64 {
	if adjustedToUTC {
		return t.UnixNano()
	} else {
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, t.Location())
		return t.Sub(epoch).Nanoseconds()
	}
}

func Nanos2Time(nanos int64, adjustedToUTC bool) time.Time {
	if adjustedToUTC {
		return time.Unix(0, nanos).UTC()

	}else{
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
		t := epoch.Add(time.Nanosecond * time.Duration(nanos))
		return t
	}
}

func Time2INT96(t time.Time) string {

}

func INT962Time(int96 string) time.Time {

}