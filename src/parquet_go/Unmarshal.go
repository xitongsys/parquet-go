package parquet_go

/*
import (
	"log"
	"parquet"
	"reflect"
)


func Unmarshal(dst []interface{}, tableMap map[string]*Table, schemaHandler *SchemaHandler) {

	if reflect.TypeOf(dst).Kind() != reflect.Slice {
		log.Panicln("The destination object must be a slice.")
		return
	}

	recordType := reflect.TypeOf(dst).Elem()
	resRef := reflect.MakeSlice(recordType, 0, 0)

	pos := map[string]Int32
	for name, _ := range tableMap {
		pos[name] = 0
	}

	for {
		obj := reflect.New(recordType)
		for name,table := range tableMap {
			path := StrToPath(name)
			lnP := len(path)
			pathRepetitionIndex := make([]int32, lnP)

			flag := 1
			i := 0
			for i=pos[name]; (flag>0 && i<len(table.Values)) ; i++ {
				flag *= table.RepetitionLevels[i]
				p := obj
				curRL := 0
				curDL := 0

				for j:=1; j<lnP; j++ {
					curRL = schemaHandler.MaxRepetitionLevel(path[:j+1])
					curDL = schemaHandler.MaxDefinitionLevel(path[:j+1])
					nodeName := path[j]
					if p.Type().Kind() == reflect.Struct {
						p = p.FieldByName(nodeName)
					}else if p.Type().Kind() == reflect.Slice {
						if p.Len() <= pathRepetitionIndex[j] {
							p = reflect.Append(p, reflect.New(p.Type().Elem()).Elem())
							p = p.Index(p.Len()-1)
							pathRepetitionIndex[j] = p.Len()
						}else{
							p = p.Index(pathRepetitionIndex[j])
						}
					}
					if curDL >= table.DefinitionLevels[i] {
						break;
					}
				}



			}
			pos[name] = i
		}
	}

}
*/
