package marshal

import (
	"reflect"
	"testing"
)

type marshalCases struct {
	nullPtr    *int
	integerPtr *int
}

func TestParquetPtrMarshal(t *testing.T) {
	ptrMarshal := &ParquetPtr{}
	var integer int = 10
	c := &marshalCases{
		integerPtr: &integer,
	}
	// case1: null ptr
	nodeNilPtr := &Node{
		Val:     reflect.ValueOf(c).Elem().FieldByName("nullPtr"),
		PathMap: nil,
		RL:      2,
		DL:      3,
	}
	res := ptrMarshal.Marshal(nodeNilPtr, nil)
	if len(res) != 0 {
		t.Errorf("Fail expect nodes len %v, get %v", 0, len(res))
	}

	// case2 not null ptr
	nodeIntPtr := &Node{
		Val:     reflect.ValueOf(c).Elem().FieldByName("integerPtr"),
		PathMap: nil,
		RL:      2,
		DL:      3,
	}
	res = ptrMarshal.Marshal(nodeIntPtr, nil)
	if len(res) != 1 || res[0].DL != 4 {
		t.Errorf("Fail expect nodes len %v, DL value %v, get nodes len %v, DL value %v", 1, 4, len(res), res[0].DL)
	}
}
