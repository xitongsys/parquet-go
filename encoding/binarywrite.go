package encoding

import (
	"fmt"
	"io"
	"math"
)

// LittleEndian
func BinaryWriteINT32(w io.Writer, nums []interface{}) error {
	buf := make([]byte, len(nums)*4)
	for i, n := range nums {
		tmp, ok := n.(int32)
		if !ok {
			return fmt.Errorf("[%v] is not int32", n)
		}
		v := uint32(tmp)
		buf[i*4+0] = byte(v)
		buf[i*4+1] = byte(v >> 8)
		buf[i*4+2] = byte(v >> 16)
		buf[i*4+3] = byte(v >> 24)
	}

	_, err := w.Write(buf)
	return err
}

func BinaryWriteINT64(w io.Writer, nums []interface{}) error {
	buf := make([]byte, len(nums)*8)
	for i, n := range nums {
		tmp, ok := n.(int64)
		if !ok {
			return fmt.Errorf("[%v] is not int64", n)
		}
		v := uint64(tmp)
		buf[i*8+0] = byte(v)
		buf[i*8+1] = byte(v >> 8)
		buf[i*8+2] = byte(v >> 16)
		buf[i*8+3] = byte(v >> 24)
		buf[i*8+4] = byte(v >> 32)
		buf[i*8+5] = byte(v >> 40)
		buf[i*8+6] = byte(v >> 48)
		buf[i*8+7] = byte(v >> 56)
	}

	_, err := w.Write(buf)
	return err
}

func BinaryWriteFLOAT32(w io.Writer, nums []interface{}) error {
	buf := make([]byte, len(nums)*4)
	for i, n := range nums {
		tmp, ok := n.(float32)
		if !ok {
			return fmt.Errorf("[%v] is not float32", n)
		}
		v := math.Float32bits(tmp)
		buf[i*4+0] = byte(v)
		buf[i*4+1] = byte(v >> 8)
		buf[i*4+2] = byte(v >> 16)
		buf[i*4+3] = byte(v >> 24)
	}

	_, err := w.Write(buf)
	return err
}

func BinaryWriteFLOAT64(w io.Writer, nums []interface{}) error {
	buf := make([]byte, len(nums)*8)
	for i, n := range nums {
		tmp, ok := n.(float64)
		if !ok {
			return fmt.Errorf("[%v] is not float32", n)
		}
		v := math.Float64bits(tmp)
		buf[i*8+0] = byte(v)
		buf[i*8+1] = byte(v >> 8)
		buf[i*8+2] = byte(v >> 16)
		buf[i*8+3] = byte(v >> 24)
		buf[i*8+4] = byte(v >> 32)
		buf[i*8+5] = byte(v >> 40)
		buf[i*8+6] = byte(v >> 48)
		buf[i*8+7] = byte(v >> 56)
	}

	_, err := w.Write(buf)
	return err
}
