package ParquetType

import (
	"fmt"
	"github.com/xitongsys/parquet-go/parquet"
	"log"
	"reflect"
)

//base type
type BOOLEAN bool
type INT32 int32
type INT64 int64
type INT96 string // length=96
type FLOAT float32
type DOUBLE float64
type BYTE_ARRAY string
type FIXED_LEN_BYTE_ARRAY string

//logical type
type UTF8 string
type INT_8 int32
type INT_16 int32
type INT_32 int32
type INT_64 int64
type UINT_8 uint32
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

func ParquetTypeToGoType(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	typeName := reflect.TypeOf(value).Name()
	switch typeName {
	case "BOOLEAN":
		return bool(value.(BOOLEAN))
	case "INT32":
		return int32(value.(INT32))
	case "INT64":
		return int64(value.(INT64))
	case "INT96":
		return string(value.(INT96))
	case "FLOAT":
		return float32(value.(FLOAT))
	case "DOUBLE":
		return float64(value.(DOUBLE))
	case "BYTE_ARRAY":
		return string(value.(BYTE_ARRAY))
	case "FIXED_LEN_BYTE_ARRAY":
		return string(value.(FIXED_LEN_BYTE_ARRAY))
	}
	return nil
}

//Scan a string to parquet value
func StrToParquetType(s string, typeName string) interface{} {
	if typeName == "BOOLEAN" {
		var v BOOLEAN
		fmt.Sscanf(s, "%t", &v)
		return v
	} else if typeName == "INT32" ||
		typeName == "INT_8" || typeName == "INT_16" || typeName == "INT_32" ||
		typeName == "UINT_8" || typeName == "UINT_16" || typeName == "UINT_32" ||
		typeName == "DATE" || typeName == "TIME_MILLIS" {
		var v INT32
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "INT64" ||
		typeName == "INT_64" || typeName == "UINT_64" ||
		typeName == "TIME_MICROS" || typeName == "TIMESTAMP_MILLIS" || typeName == "TIMESTAMP_MICROS" {
		var v INT64
		fmt.Sscanf(s, "%d", &v)
		return v
	} else if typeName == "INT96" {
		var v INT96
		v = INT96(s)
		return v
	} else if typeName == "FLOAT" {
		var v FLOAT
		fmt.Sscanf(s, "%f", &v)
		return v
	} else if typeName == "DOUBLE" {
		var v DOUBLE
		fmt.Sscanf(s, "%f", &v)
		return v
	} else if typeName == "BYTE_ARRAY" || typeName == "UTF8" || typeName == "INTERVAL" || typeName == "DECIMAL" {
		var v BYTE_ARRAY
		v = BYTE_ARRAY(s)
		return v
	} else if typeName == "FIXED_LEN_BYTE_ARRAY" {
		var v FIXED_LEN_BYTE_ARRAY
		v = FIXED_LEN_BYTE_ARRAY(s)
		return v
	} else {
		log.Printf("Type Error: %v ", typeName)
		return nil
	}
}

func GoTypeToParquetType(src interface{}, pT *parquet.Type, cT *parquet.ConvertedType) interface{} {
	if cT == nil {
		if *pT == parquet.Type_BOOLEAN {
			return BOOLEAN(src.(bool))
		} else if *pT == parquet.Type_INT32 {
			return INT32(src.(int32))
		} else if *pT == parquet.Type_INT64 {
			return INT64(src.(int64))
		} else if *pT == parquet.Type_INT96 {
			return INT96(src.(string))
		} else if *pT == parquet.Type_FLOAT {
			return FLOAT(src.(float32))
		} else if *pT == parquet.Type_DOUBLE {
			return DOUBLE(src.(float64))
		} else if *pT == parquet.Type_BYTE_ARRAY {
			return BYTE_ARRAY(src.(string))
		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return FIXED_LEN_BYTE_ARRAY(src.(string))
		}
		return nil
	}

	if *cT == parquet.ConvertedType_UTF8 {
		return BYTE_ARRAY(src.(string))
	} else if *cT == parquet.ConvertedType_INT_8 {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_INT_16 {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_INT_32 {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_INT_64 {
		return INT64(src.(int64))
	} else if *cT == parquet.ConvertedType_UINT_8 {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_UINT_16 {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_UINT_32 {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_UINT_64 {
		return INT64(src.(int64))
	} else if *cT == parquet.ConvertedType_DATE {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_TIME_MILLIS {
		return INT32(src.(int32))
	} else if *cT == parquet.ConvertedType_TIME_MICROS {
		return INT64(src.(int64))
	} else if *cT == parquet.ConvertedType_TIMESTAMP_MILLIS {
		return INT64(src.(int64))
	} else if *cT == parquet.ConvertedType_TIMESTAMP_MICROS {
		return INT64(src.(int64))
	} else if *cT == parquet.ConvertedType_INTERVAL {
		return FIXED_LEN_BYTE_ARRAY(src.(string))
	} else if *cT == parquet.ConvertedType_DECIMAL {
		return BYTE_ARRAY(src.(string))
	} else {
		return nil
	}
}
