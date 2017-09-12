package Common

import (
	//	"log"
	. "ParquetType"
	"parquet"
	"reflect"
	"strings"
)

func BitNum(num uint64) uint64 { //the number of bits needed by the num; 0 needs 0, 1 need 1, 2 need 2, 3 need 2 ....
	var bitn int32 = 63
	for (bitn >= 0) && (((uint64(1) << uint32(bitn)) & num) == 0) {
		bitn--
	}
	return uint64(bitn + 1)
}

func Cmp(ai interface{}, bi interface{}) int {
	if ai == nil && bi != nil {
		return -1
	} else if ai == nil && bi == nil {
		return 0
	} else if ai != nil && bi == nil {
		return 1
	}

	name := reflect.TypeOf(a).Name()
	switch name {
	case "BOOLEAN":
		a, b := 0, 0
		if ai.(BOOLEAN) {
			a = 1
		}
		if bi.(BOOLEAN) {
			b = 1
		}
		return a - b

	case "INT32":
		a, b := ai.(INT32), bi.(INT32)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT64":
		a, b := ai.(INT32), bi.(INT32)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT96":
	case "FLOAT":
		a, b := ai.(FLOAT), bi.(FLOAT)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "DOUBLE":
		a, b := ai.(FLOAT), bi.(FLOAT)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "BYTE_ARRAY":
		a, b := ai.(BYTE_ARRAY), bi.(BYTE_ARRAY)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "FIXED_LEN_BYTE_ARRAY":
		a, b := ai.(FIXED_LEN_BYTE_ARRA), bi.(FIXED_LEN_BYTE_ARRA)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "UTF8":
		a, b := ai.(UTF8), bi.(UTF8)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT_8":
		a, b := ai.(INT_8), bi.(INT_8)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT_16":
		a, b := ai.(INT_16), bi.(INT_16)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT_32":
		a, b := ai.(INT_32), bi.(INT_32)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT_64":
		a, b := ai.(INT_64), bi.(INT_64)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "UINT_8":
		a, b := ai.(UINT_8), bi.(UINT_8)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "UINT_16":
		a, b := ai.(UINT_16), bi.(UINT_16)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "UINT_32":
		a, b := ai.(UINT_32), bi.(UINT_32)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "UINT_64":
		a, b := ai.(UINT_64), bi.(UINT_64)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "DATE":
		a, b := ai.(DATE), bi.(DATE)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "TIME_MILLIS":
		a, b := ai.(TIME_MILLIS), bi.(TIME_MILLIS)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "TIME_MICROS":
		a, b := ai.(TIME_MICROS), bi.(TIME_MICROS)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "TIMESTAMP_MILLS":
		a, b := ai.(TIMESTAMP_MILLS), bi.(TIMESTAMP_MILLS)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "TIMESTAMP_MICROS":
		a, b := ai.(TIMESTAMP_MICROS), bi.(TIMESTAMP_MICROS)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INTERVAL":

	case "DCEIMAL":

	}

}

func Min(a interface{}, b interface{}) interface{} {
	if a == nil {
		return b
	} else if b == nil {
		return a
	}

}

func SizeOf(val reflect.Value) int64 {
	switch val.Type().Kind() {
	case reflect.Int16:
		return 2
	case reflect.Int32:
		return 4
	case reflect.Int64:
		return 8
	case reflect.Float32:
		return 4
	case reflect.Float64:
		return 8
	case reflect.Bool:
		return 1
	case reflect.String:
		return int64(val.Len())
	case reflect.Slice:
		var size int64 = 0
		for i := 0; i < val.Len(); i++ {
			size += SizeOf(val.Index(i))
		}
		return size
	case reflect.Struct:
		var size int64 = 0
		numField := TypeNumberField(val.Type())
		for i := 0; int32(i) < numField; i++ {
			size += SizeOf(val.Field(i))
		}
		return size
	default:
		return 4
	}
}

func PathToStr(path []string) string {
	return strings.Join(path, ".")
}

func StrToPath(str string) []string {
	return strings.Split(str, ".")
}

func TypeNumberField(t reflect.Type) int32 {
	if t.Kind() == reflect.Struct {
		return int32(t.NumField())
	} else if t.Kind() == reflect.Slice {
		return 1
	} else {
		return 0
	}
}

func GoTypeToParquetType(goT reflect.Type) parquet.Type {
	switch goT.Kind() {
	case reflect.Bool:
		return parquet.Type_BOOLEAN
	case reflect.Int:
		return parquet.Type_INT64
	case reflect.Int32:
		return parquet.Type_INT32
	case reflect.Int64:
		return parquet.Type_INT64
	case reflect.Float32:
		return parquet.Type_FLOAT
	case reflect.Float64:
		return parquet.Type_DOUBLE
	case reflect.String:
		return parquet.Type_BYTE_ARRAY
	default:
		return parquet.Type_FIXED_LEN_BYTE_ARRAY
	}
}
