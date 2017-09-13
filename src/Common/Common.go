package Common

import (
	//	"log"
	. "ParquetType"
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

	name := reflect.TypeOf(ai).Name()
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
		a, b := ai.(INT64), bi.(INT64)
		if a > b {
			return 1
		} else if a < b {
			return -1
		}
		return 0

	case "INT96":
		a, b := []byte(ai.(INT96)), []byte(bi.(INT96))
		fa, fb := (a[11] >> 7), (b[11] >> 7)
		if fa > fb {
			return -1
		} else if fa < fb {
			return 1
		} else if fa == 1 {
			for i := 11; i >= 0; i-- {
				if a[i] > b[i] {
					return -1
				} else if a[i] < b[i] {
					return 1
				}
			}
			return 0
		} else {
			for i := 11; i >= 0; i-- {
				if a[i] > b[i] {
					return 1
				} else if a[i] < b[i] {
					return -1
				}
			}
			return 0
		}

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
		a, b := ai.(FIXED_LEN_BYTE_ARRAY), bi.(FIXED_LEN_BYTE_ARRAY)
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

	case "TIMESTAMP_MILLIS":
		a, b := ai.(TIMESTAMP_MILLIS), bi.(TIMESTAMP_MILLIS)
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
		a, b := []byte(ai.(INTERVAL)), []byte(bi.(INTERVAL))
		for i := 11; i >= 0; i-- {
			if a[i] > b[i] {
				return 1
			} else if a[i] < b[i] {
				return -1
			}
		}
		return 0

	case "DCEIMAL":
		a, b := []byte(ai.(DECIMAL)), []byte(bi.(DECIMAL))
		fa, fb := (a[0] >> 7), (b[0] >> 7)
		la, lb := len(a), len(b)
		if fa > fb {
			return -1
		} else if fa < fb {
			return 1
		} else {
			i, j := 0, 0
			for i < la || j < lb {
				ba, bb := byte(0x0), byte(0x0)
				if i < la {
					ba = a[i]
					i++
				}
				if j < lb {
					bb = b[j]
					j++
				}
				if ba > bb {
					if fa == 1 {
						return -1
					} else {
						return 1
					}
				} else if ba < bb {
					if fa == 1 {
						return 1
					} else {
						return -1
					}
				}
			}
			return 0
		}
	}
	return 0
}

func Max(a interface{}, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if Cmp(a, b) > 0 {
		return a
	}
	return b
}

func Min(a interface{}, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if Cmp(a, b) > 0 {
		return b
	}
	return a
}

func SizeOf(val reflect.Value) int64 {
	tk := val.Type().Kind()
	if tk == reflect.Slice {
		var size int64 = 0
		for i := 0; i < val.Len(); i++ {
			size += SizeOf(val.Index(i))
		}
		return size
	} else if tk == reflect.Struct {
		var size int64 = 0
		for i := 0; i < val.Type().NumField(); i++ {
			size += SizeOf(val.Field(i))
		}
		return size

	} else if tk == reflect.Map {
		var size int64 = 0
		keys := val.MapKeys()
		for i := 0; i < len(keys); i++ {
			size += SizeOf(keys[i])
			size += SizeOf(val.MapIndex(keys[i]))
		}
		return size
	}

	switch val.Type().Name() {
	case "BOOLEAN":
		return 1
	case "INT32":
		return 4
	case "INT64":
		return 8
	case "INT96":
		return 12
	case "FLOAT":
		return 4
	case "DOUBLE":
		return 8
	case "BYTE_ARRAY":
		return int64(val.Len())
	case "FIXED_LEN_BYTE_ARRAY":
		return int64(val.Len())
	case "UTF8":
		return int64(val.Len())
	case "INT_8":
		return 4
	case "INT_16":
		return 4
	case "INT_32":
		return 4
	case "INT_64":
		return 8
	case "UINT_8":
		return 4
	case "UINT_16":
		return 4
	case "UINT_32":
		return 4
	case "UINT_64":
		return 8
	case "DATE":
		return 4
	case "TIME_MILLIS":
		return 4
	case "TIME_MICROS":
		return 8
	case "TIMESTAMP_MILLIS":
		return 8
	case "TIMESTAMP_MICROS":
		return 8
	case "INTERVAL":
		return 12
	case "DECIMAL":
		return int64(val.Len())
	}

	return 4
}

func PathToStr(path []string) string {
	return strings.Join(path, ".")
}

func StrToPath(str string) []string {
	return strings.Split(str, ".")
}
