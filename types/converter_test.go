package types

import (
	"testing"
	"time"
)

func TestINT96(t *testing.T) {
	t1 := time.Now().Truncate(time.Microsecond).UTC()
	s := TimeToINT96(t1)
	t2 := INT96ToTime(s)

	if !t1.Equal(t2) {
		t.Error("INT96 error: ", t1, t2)
	}

}