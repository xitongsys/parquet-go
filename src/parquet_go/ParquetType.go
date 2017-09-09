package parquet_go

import (
	"parquet"
)

//base type
type BOOLEAN bool
type INT32 int64
type INT64 int64
type INT96 string // length=96
type FLOAT float32
type DOUBLE float64
type BYTE_ARRAY string
type FIXED_LEN_BYTE_ARRAY string

//logical type
type UTF8 string
type INT_8 byte
type INT_16 int32
type INT_32 int32
type INT_64 int64
type UINT_8 byte
type UINT_16 uint32
type UINT_32 uint32
type UINT_64 uint64
type DATE int32
type TIME_MILLIS int32
type TIME_MICROS int64
type TIMESTAMP_MILLIS int64
type TIMESTAMP_MICROS int64
type INTERVAL string // length=12
type DECIMAL string

func NameToBaseType(name string) parquet.Type {
	switch name {
	case "BOOLEAN":
		return parquet.Type_BOOLEAN
	case "INT32":
		return parquet.Type_INT32
	case "INT64":
		return parquet.Type_INT64
	case "INT96":
		return parquet.Type_INT96
	case "FLOAT":
		return parquet.Type_FLOAT
	case "DOUBLE":
		return parquet.Type_DOUBLE
	case "BYTE_ARRAY":
		return parquet.Type_BYTE_ARRAY
	default:
		return parquet.Type_FIXED_LEN_BYTE_ARRAY
	}
}

func NameToConvertedType(name string) parquet.ConvertedType {
	switch name {
	case "UTF8":
		return parquet.ConvertedType_UTF8
	case "INT_8":
		return parquet.ConvertedType_INT_8
	case "INT_16":
		return parquet.ConvertedType_INT_16
	case "INT_32":
		return parquet.ConvertedType_INT_32
	case "INT_64":
		return parquet.ConvertedType_INT_64
	case "UINT_8":
		return parquet.ConvertedType_UINT_8
	case "UINT_16":
		return parquet.ConvertedType_UINT_16
	case "UINT_32":
		return parquet.ConvertedType_UINT_32
	case "UINT_64":
		return parquet.ConvertedType_UINT_64
	case "DATE":
		return parquet.ConvertedType_DATE
	case "TIME_MILLIS":
		return parquet.ConvertedType_TIME_MILLIS
	case "TIME_MICROS":
		return parquet.ConvertedType_TIME_MICROS
	case "TIMESTAMP_MILLIS":
		return parquet.ConvertedType_TIMESTAMP_MILLIS
	case "TIMESTAMP_MICROS":
		return parquet.ConvertedType_TIMESTAMP_MICROS
	case "INTERVAL":
		return parquet.ConvertedType_INTERVAL
	case "DECIMAL":
		return parquet.ConvertedType_DECIMAL
	default:
		return parquet.ConvertedType_UTF8
	}

}

func IsBaseType(name string) boolean {
	if name == "BOOLEAN" ||
		name == "INT32" || name == "INT64" || name == "INT96" ||
		name == "FLOAT" || name == "DOUBLE" ||
		name == "BYTE_ARRAY" || name == "FIXED_LEN_BYTE_ARRAY" {
		return true
	}
	return false
}
