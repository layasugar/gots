package gots

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"reflect"
	"strings"
)

// 添加column
func addPutRowColumn(columns []attributeColumn, putRowChange *tablestore.PutRowChange) error {
	for _, column := range columns {
		putRowChange.AddColumn(column.columnName, column.value)
	}
	return nil
}

// 添加column
func addUpdateRowColumn(columns []attributeColumn, updateRowChange *tablestore.UpdateRowChange) error {
	for _, column := range columns {
		t := reflect.ValueOf(column.value)
		switch t.Kind() {

		case reflect.String:
			updateRowChange.PutColumn(column.columnName, t.String())

		case reflect.Bool:
			updateRowChange.PutColumn(column.columnName, t.Bool())

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			updateRowChange.PutColumn(column.columnName, t.Int())

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			updateRowChange.PutColumn(column.columnName, int64(t.Uint()))

		case reflect.Float64, reflect.Float32:
			updateRowChange.PutColumn(column.columnName, t.Float())

		default:
			return errors.New("column类型错误,只支持int和string和bool和double")
		}
	}
	return nil
}

// 按照sep个数拆分slice[]int64为map[int][]int64
func splitRows(s []tablestore.RowChange, sep int) map[int][]tablestore.RowChange {
	if sep <= 0 {
		return map[int][]tablestore.RowChange{0: s}
	}
	var res = make(map[int][]tablestore.RowChange, len(s)/sep+1)
	if len(s) <= sep {
		res[0] = s
	} else {
		for i := 0; i < len(s); i += sep {
			if i+sep >= len(s) {
				res[i] = s[i:]
			} else {
				res[i] = s[i : i+sep]
			}
		}
	}
	return res
}

func retryBatchOtsOrder(client *tablestore.TableStoreClient, batchWriteReq *tablestore.BatchWriteRowRequest, table string) {
	rows, ok := batchWriteReq.RowChangesGroupByTable[table]
	if !ok {
		fmt.Printf("retryBatchOtsOrder failed tableName is not ok")
		return
	}
	mapRows := splitArgRows(rows)
	for _, sRows := range mapRows {
		batchWriteReq.RowChangesGroupByTable[table] = sRows
		response, err := client.BatchWriteRow(batchWriteReq)
		if err != nil {
			if len(sRows) != 1 {
				errStrBool := strings.Contains(err.Error(), postLimitSizeDoc)
				if errStrBool {
					retryBatchOtsOrder(client, batchWriteReq, table)
				} else {
					fmt.Printf("batch request failed with: %v, %s, count: %d条", response, err.Error(), len(sRows))
				}
			}
		} else {
			//fmt.Printf("batch request success with count: %d条", len(sRows))
		}
	}
	return
}

func splitArgRows(s []tablestore.RowChange) map[int][]tablestore.RowChange {
	var res = make(map[int][]tablestore.RowChange, 2)
	lens := len(s)
	if lens <= 1 {
		res[lens] = s
		return res
	}
	sep := lens / 2
	res[sep] = s[:sep]
	res[lens] = s[sep:]
	return res
}

func getReflectValue(reflectValue reflect.Value) (value interface{}, err error) {
	switch reflectValue.Type().String() {
	case "int", "int8", "int16", "int32", "int64":
		value = reflectValue.Int()
	case "uint", "uint8", "uint16", "uint32", "uint64":
		value = int64(reflectValue.Uint())
	case "float32", "float64":
		value = reflectValue.Float()
	case "string":
		value = reflectValue.String()
	case "bool":
		value = reflectValue.Bool()
	case "uintptr", "ptr", "complex64", "complex128", "array", "chan", "func", "interface", "map", "slice", "struct":
		err = errors.New("getReflectValue type error")
		return
	default:
		err = errors.New("getReflectValue type error")
		return
	}
	return
}

func getPkValue(reflectValue reflect.Value) (int64, error) {
	switch reflectValue.Type().String() {
	case "int", "int8", "int16", "int32", "int64":
		return reflectValue.Int(), nil
	case "uint", "uint8", "uint16", "uint32", "uint64":
		return int64(reflectValue.Uint()), nil
	}
	return 0, errors.New("getPkValue unknown type")
}

func getRecursionReflectValue(columns *[]attributeColumn, v reflect.Value) *[]attributeColumn {
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Kind() == reflect.Struct {
			getRecursionReflectValue(columns, v.Field(i))
		} else {
			tags := v.Type().Field(i).Tag.Get("json")
			tagSlice := strings.Split(tags, ",")
			if tagSlice[0] != "-" && tagSlice[0] != "" {
				var tmp attributeColumn
				columnValue, err := getReflectValue(v.Field(i))
				if err != nil {
					continue
				}
				tmp.value = columnValue
				tmp.columnName = tagSlice[0]
				*columns = append(*columns, tmp)
			}
		}
	}
	return columns
}

func getHashKey(v ...string) string {
	var keyStr string
	for _, item := range v {
		keyStr += item
	}

	return Md5(keyStr)
}

// md5
func Md5(s string) string {
	m := md5.Sum([]byte(s))
	return hex.EncodeToString(m[:])
}
