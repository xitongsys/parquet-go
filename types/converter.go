package types

import (
	"encoding/binary"
	"math/big"
	"time"
)

func TimeToTIME_MILLIS(t time.Time, adjustedToUTC bool) int64 {
	return TimeToTIME_MICROS(t, adjustedToUTC) / 1000
}

func TimeToTIME_MICROS(t time.Time, adjustedToUTC bool) int64 {
	if adjustedToUTC {
		tu := t.UTC()
		h, m, s, ns := int64(tu.Hour()), int64(tu.Minute()), int64(tu.Second()), int64(tu.Nanosecond())
		nanos := h * int64(time.Hour) + m * int64(time.Minute) + s * int64(time.Second) + ns * int64(time.Nanosecond)
		return nanos / 1000

	} else {
		h, m, s, ns := int64(t.Hour()), int64(t.Minute()), int64(t.Second()), int64(t.Nanosecond())
		nanos := h * int64(time.Hour) + m * int64(time.Minute) + s * int64(time.Second) + ns * int64(time.Nanosecond)
		return nanos / 1000
	}
}

func TimeToTIMESTAMP_MILLIS(t time.Time, adjustedToUTC bool) int64 {
	return TimeToTIMESTAMP_MICROS(t, adjustedToUTC) / 1000
}

func TIMESTAMP_MILLISToTime(millis int64, adjustedToUTC bool) time.Time {
	return TIMESTAMP_MICROSToTime(millis * 1000, adjustedToUTC)
} 

func TimeToTIMESTAMP_MICROS(t time.Time, adjustedToUTC bool) int64 {
	return TimeToTIMESTAMP_NANOS(t, adjustedToUTC) / 1000
}

func TIMESTAMP_MICROSToTime(micros int64, adjustedToUTC bool) time.Time {
	return TIMESTAMP_NANOSToTime(micros * 1000, adjustedToUTC)
}

func TimeToTIMESTAMP_NANOS(t time.Time, adjustedToUTC bool) int64 {
	if adjustedToUTC {
		return t.UnixNano()
	} else {
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, t.Location())
		return t.Sub(epoch).Nanoseconds()
	}
}

func TIMESTAMP_NANOSToTime(nanos int64, adjustedToUTC bool) time.Time {
	if adjustedToUTC {
		return time.Unix(0, nanos).UTC()

	}else{
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)
		t := epoch.Add(time.Nanosecond * time.Duration(nanos))
		return t
	}
}

// Reports the Julian Day Number for t. Note that Julian days start at 12:00 UTC.
//
// Code from https://github.com/rickar/cal/blob/6dbb2c016a010db6388f3ffc6b408e05306c4f9d/v2/cal_funcs.go#L122
func julianDayNumber(t time.Time) int {
	utc := t.UTC()
	a := (14 - int(utc.Month())) / 12
	y := utc.Year() + 4800 - a
	m := int(utc.Month()) + 12*a - 3

	jdn := utc.Day() + (153*m+2)/5 + 365*y + y/4 - y/100 + y/400 - 32045
	if utc.Hour() < 12 {
		jdn--
	}
	return jdn
}

// Reports the INT96 Timestamp in string format as required by Spark for Parquet files.
//
// Reference: https://stackoverflow.com/questions/53103762/cast-int96-timestamp-from-parquet-to-golang/53104516#53104516
func TimeToINT96(t time.Time) string {
	utc := t.UTC()
	jdn := julianDayNumber(utc)
	seconds := time.Duration(utc.Hour()*3600+utc.Minute()*60+utc.Second()) * time.Second

	bs1 := make([]byte, 4)
	binary.BigEndian.PutUint32(bs1, uint32(jdn))

	bs2 := make([]byte, 8)
	binary.BigEndian.PutUint64(bs2, uint64(seconds.Nanoseconds()))

	bs := append(bs1, bs2...)
	return new(big.Int).SetBytes(bs).String()
}

func INT96ToTime(int96 string) time.Time {
	nano := binary.LittleEndian.Uint64([]byte(int96[:8]))
	dt := binary.LittleEndian.Uint32([]byte(int96[8:]))

	l := dt + 68569
	n := 4 * l / 146097
	l = l - (146097*n+3)/4
	i := 4000 * (l + 1) / 1461001
	l = l - 1461*i/4 + 31
	j := 80 * l / 2447
	k := l - 2447*j/80
	l = j / 11
	j = j + 2 - 12*l
	i = 100*(n-49) + i + l
	tm := time.Date(int(i), time.Month(j), int(k), 0, 0, 0, 0, time.UTC)
	tm = tm.Add(time.Duration(nano))
	return tm
}