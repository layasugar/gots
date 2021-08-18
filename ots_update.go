package gots

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"reflect"
	"strings"
)

// 主键更新1条
func (c *tableStoreClient) Update(value interface{}, pkValue ...int64) *updateRequest {
	var ir = &updateRequest{
		dwSearchOrm: c.dwSearchOrm,
		tableName:   c.tableName,
	}

	ir.setColumns(value)

	if len(pkValue) > 0 {
		ir.pkValue = pkValue[0]
	}
	return ir
}

func (c *updateRequest) setColumns(value interface{}) *updateRequest {
	reflectValue := reflect.Indirect(reflect.ValueOf(value))
	typeValue := reflectValue.Type()
	switch reflectValue.Kind() {
	case reflect.Struct:
		for i := 0; i < reflectValue.NumField(); i++ {
			if reflectValue.Field(i).IsZero() {
				continue
			}

			tags := typeValue.Field(i).Tag.Get("json")
			pkSlice := strings.Split(tags, ",")
			if pkSlice[0] == pkName {
				pkValue, err := getPkValue(reflectValue.Field(i))
				if err == nil {
					c.pkValue = pkValue
				}
				continue
			}

			if pkSlice[0] == "" {
				if reflectValue.Field(i).Kind() == reflect.Struct {
					var tmp []attributeColumn
					getRecursionReflectValue(&tmp, reflectValue.Field(i))
					c.columns = append(c.columns, tmp...)
				}
			}

			if pkSlice[0] != "-" && pkSlice[0] != "" {
				var tmp attributeColumn
				columnValue, err := getReflectValue(reflectValue.Field(i))
				if err != nil {
					continue
				}
				tmp.value = columnValue
				tmp.columnName = pkSlice[0]
				c.columns = append(c.columns, tmp)
			}
		}
	case reflect.Map:
		columnsMap, ok := value.(map[string]interface{})
		if !ok {
			c.err = errors.New("insert只支持map[string]interface和结构体")
		}
		for columnName, columnValue := range columnsMap {
			if columnName == pkName {
				t := reflect.ValueOf(columnValue)
				thisColumnValue, err := getPkValue(t)
				if err == nil {
					c.pkValue = thisColumnValue
				}
				continue
			} else {
				t := reflect.ValueOf(columnValue)
				thisColumnValue, err := getReflectValue(t)
				if err != nil {
					continue
				}

				var tmp = attributeColumn{
					columnName: columnName,
					value:      thisColumnValue,
				}
				c.columns = append(c.columns, tmp)
			}
		}

	default:
		c.err = errors.New("insert只支持map[string]interface和struct")
	}
	return c
}

func (c *updateRequest) Do() (*int32, error) {
	var err error
	if c.err != nil {
		return nil, err
	}

	if c.dwSearchOrm.err != nil {
		return nil, c.dwSearchOrm.err
	}

	if c.pkValue == 0 {
		return nil, errors.New("pk是必须的")
	}

	if len(c.columns) < 1 {
		return nil, errors.New("columns是必须的")
	}

	updateRowRequest := new(tablestore.UpdateRowRequest)
	updateRowChange := new(tablestore.UpdateRowChange)
	updateRowChange.TableName = c.tableName
	updateRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)

	updatePk := new(tablestore.PrimaryKey)
	updatePk.AddPrimaryKeyColumn(pkName, c.pkValue)

	err = addUpdateRowColumn(c.columns, updateRowChange)
	if err != nil {
		return nil, err
	}

	updateRowChange.PrimaryKey = updatePk
	updateRowRequest.UpdateRowChange = updateRowChange
	resp, err := c.dwSearchOrm.client.UpdateRow(updateRowRequest)
	if err != nil {
		return nil, err
	}
	var counts int32
	if resp != nil && resp.ConsumedCapacityUnit != nil {
		counts = resp.ConsumedCapacityUnit.Write
	}
	return &counts, nil
}
