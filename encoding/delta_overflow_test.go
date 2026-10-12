package encoding

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
)

func TestDeltaINT64RoundTripWhenFrameOverflows(t *testing.T) {
	// Issue 477: a frame wider than MaxInt64 was given a bit width from the
	// signed comparison, so the high bit was dropped and later values changed.
	reported := []interface{}{
		int64(3328451335138149956),
		int64(5263531936693774911),
		int64(7955079406183515637),
		int64(2703501726821866378),
		int64(2740103009342231109),
		int64(6941261091797652072),
		int64(1905388747193831650),
	}
	assertDeltaINT64RoundTrip(t, reported)
	assertDeltaINT64RoundTrip(t, []interface{}{int64(math.MaxInt64), int64(math.MinInt64), int64(math.MaxInt64)})

	r := rand.New(rand.NewSource(1))
	for n := 1; n <= 129; n++ {
		vals := make([]interface{}, n)
		for i := 0; i < n; i++ {
			vals[i] = r.Int63()
		}
		assertDeltaINT64RoundTrip(t, vals)
	}
}

func TestDeltaINT32RoundTripWhenFrameOverflows(t *testing.T) {
	// 2000000000 - (-2000000000) does not fit in int32. The signed max stayed 0 and the frame was stored in 0 bits.
	assertDeltaINT32RoundTrip(t, []interface{}{int32(0), int32(2000000000), int32(0)})
}

func assertDeltaINT64RoundTrip(t *testing.T, vals []interface{}) {
	t.Helper()
	got, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(WriteDeltaINT64(vals)))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != len(vals) {
		t.Fatalf("len %d, want %d", len(got), len(vals))
	}
	for i := range vals {
		if got[i] != vals[i] {
			t.Fatalf("index %d got %v want %v", i, got[i], vals[i])
		}
	}
}

func assertDeltaINT32RoundTrip(t *testing.T, vals []interface{}) {
	t.Helper()
	got, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(WriteDeltaINT32(vals)))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != len(vals) {
		t.Fatalf("len %d, want %d", len(got), len(vals))
	}
	for i := range vals {
		if got[i] != vals[i] {
			t.Fatalf("index %d got %v want %v", i, got[i], vals[i])
		}
	}
}
