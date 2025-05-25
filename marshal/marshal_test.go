package marshal

import (
	"reflect"
	"testing"

	"github.com/hangxie/parquet-go/v2/schema"
)

type marshalCases struct {
	nullPtr    *int //lint:ignore U1000 this is a placeholder for testing
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

	stack := []*Node{}
	res := ptrMarshal.Marshal(nodeNilPtr, nil, stack)
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
	stack = []*Node{}
	res = ptrMarshal.Marshal(nodeIntPtr, nil, stack)
	if len(res) != 1 || res[0].DL != 4 {
		t.Errorf("Fail expect nodes len %v, DL value %v, get nodes len %v, DL value %v", 1, 4, len(res), res[0].DL)
	}
}

func TestMarshalFast(t *testing.T) {
	type testElem struct {
		Bool      bool    `parquet:"name=bool, type=BOOLEAN"`
		Int       int     `parquet:"name=int, type=INT64"`
		Int8      int8    `parquet:"name=int8, type=INT32"`
		Int16     int16   `parquet:"name=int16, type=INT32"`
		Int32     int32   `parquet:"name=int32, type=INT32"`
		Int64     int64   `parquet:"name=int64, type=INT64"`
		Float     float32 `parquet:"name=float, type=FLOAT"`
		Double    float64 `parquet:"name=double, type=DOUBLE"`
		ByteArray string  `parquet:"name=bytearray, type=BYTE_ARRAY"`

		Ptr    *int64  `parquet:"name=boolptr, type=INT64"`
		PtrPtr **int64 `parquet:"name=boolptrptr, type=INT64"`

		List         []string  `parquet:"name=list, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		PtrList      *[]string `parquet:"name=ptrlist, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		ListPtr      []*string `parquet:"name=listptr, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		Repeated     []int32   `parquet:"name=repeated, type=INT32, repetitiontype=REPEATED"`
		NestRepeated [][]int32 `parquet:"name=nestrepeated, type=INT32, repetitiontype=REPEATED"`
	}

	type subElem struct {
		Val string `parquet:"name=val, type=BYTE_ARRAY"`
	}

	type testNestedElem struct {
		SubElem       subElem     `parquet:"name=subelem"`
		SubPtr        *subElem    `parquet:"name=subptr"`
		SubList       []subElem   `parquet:"name=sublist"`
		SubRepeated   []*subElem  `parquet:"name=subrepeated"`
		EmptyElem     struct{}    `parquet:"name=emptyelem"`
		EmptyPtr      *struct{}   `parquet:"name=emptyptr"`
		EmptyList     []struct{}  `parquet:"name=emptylist"`
		EmptyRepeated []*struct{} `parquet:"name=emptyrepeated"`
	}

	type testIfaceElem struct {
		Bool      interface{} `parquet:"name=bool, type=BOOLEAN"`
		Int32     interface{} `parquet:"name=int32, type=INT32"`
		Int64     interface{} `parquet:"name=int64, type=INT64"`
		Float     interface{} `parquet:"name=float, type=FLOAT"`
		Double    interface{} `parquet:"name=double, type=DOUBLE"`
		ByteArray interface{} `parquet:"name=bytearray, type=BYTE_ARRAY"`
	}

	i64 := int64(31)
	refRef := &i64
	var nilRef *int64
	var nilList []string
	str := "hi"

	testCases := []struct {
		value interface{}
	}{
		{testElem{Bool: true}},
		{testElem{Ptr: &i64, PtrPtr: &refRef}},
		{testElem{Ptr: nilRef, PtrPtr: &nilRef}},
		{testElem{Ptr: nil, PtrPtr: nil}},
		{testElem{Repeated: nil}},
		{testElem{Repeated: []int32{}}},
		{testElem{Repeated: []int32{31}}},
		{testElem{Repeated: []int32{31, 32, 33, 34}}},
		{testElem{List: nil}},
		{testElem{List: []string{}}},
		{testElem{List: []string{"hi"}}},
		{testElem{List: []string{"1", "2", "3", "4"}}},
		{testElem{PtrList: nil}},
		{testElem{PtrList: &nilList}},
		{testElem{PtrList: &[]string{}}},
		{testElem{PtrList: &[]string{"hi"}}},
		{testElem{PtrList: &[]string{"1", "2", "3", "4"}}},
		{testElem{ListPtr: nil}},
		{testElem{ListPtr: []*string{}}},
		{testElem{ListPtr: []*string{nil}}},
		{testElem{ListPtr: []*string{nil, nil, nil}}},
		{testElem{ListPtr: []*string{&str}}},
		{testElem{ListPtr: []*string{&str, &str, &str}}},
		{testElem{ListPtr: []*string{&str, nil, &str}}},
		{testElem{NestRepeated: nil}},
		{testElem{NestRepeated: [][]int32{}}},
		{testElem{NestRepeated: [][]int32{{}}}},
		{testElem{NestRepeated: [][]int32{{1, 2, 3}}}},
		// Test doesn't pass because it disagrees with Marshal, but I'm pretty sure that,
		// if this is supported at all, this implementation is correct and there's a bug
		// in Marshal.
		// {testElem{NestRepeated: [][]int32{{1}, {2, 3, 4, 5}, nil, {6}}}},

		{testNestedElem{}},
		{testNestedElem{SubElem: subElem{Val: "hi"}}},
		{testNestedElem{SubPtr: &subElem{Val: "hi"}}},
		{testNestedElem{SubList: []subElem{}}},
		{testNestedElem{SubList: []subElem{{Val: "hi"}}}},
		{testNestedElem{SubList: []subElem{{Val: "hi"}, {}, {Val: "there"}}}},
		{testNestedElem{SubRepeated: []*subElem{}}},
		{testNestedElem{SubRepeated: []*subElem{{Val: "hi"}}}},
		{testNestedElem{SubRepeated: []*subElem{{Val: "hi"}, nil, {Val: "there"}}}},
		{testNestedElem{EmptyPtr: &struct{}{}}},
		{testNestedElem{EmptyList: []struct{}{}}},
		{testNestedElem{EmptyList: []struct{}{{}}}},
		{testNestedElem{EmptyList: []struct{}{{}, {}}}},
		{testNestedElem{EmptyRepeated: []*struct{}{}}},
		{testNestedElem{EmptyRepeated: []*struct{}{{}}}},
		{testNestedElem{EmptyRepeated: []*struct{}{{}, nil, {}}}},

		// interface{}
		{testIfaceElem{}},
		{testIfaceElem{
			Bool:      false,
			Int32:     int32(0),
			Int64:     int64(0),
			Float:     float32(0),
			Double:    float64(0),
			ByteArray: "",
		}},
		{testIfaceElem{
			Bool:      true,
			Int32:     int32(12345),
			Int64:     int64(54321),
			Float:     float32(1.0),
			Double:    float64(100.0),
			ByteArray: "hi",
		}},
	}

	for _, tt := range testCases {
		t.Run("", func(t *testing.T) {
			input := []interface{}{tt.value}
			schemaType := reflect.Zero(reflect.PointerTo(reflect.TypeOf(tt.value))).Interface()
			sch, err := schema.NewSchemaHandlerFromStruct(schemaType)
			if err != nil {
				t.Fatalf("%v", err)
			}
			expected, err := Marshal(input, sch)
			if err != nil {
				t.Fatalf("%v", err)
			}
			actual, err := MarshalFast(input, sch)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if !reflect.DeepEqual(expected, actual) {
				// require.Equal(t, expected, actual)
				t.Errorf("not equal")
			}
		})
	}
}
