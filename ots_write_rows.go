package gots

import (
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"reflect"
	"strings"
)

// 写入多条，更新多条，删除多条
func (c *tableStoreClient) WriteRows() *writeRowsRequest {
	var ir = &writeRowsRequest{
		dwSearchOrm: c.dwSearchOrm,
		tableName:   c.tableName,
	}

	return ir
}

func (c *writeRowsRequest) SetPutData(data interface{}, pks ...[]int64) *writeRowsRequest {
	reflectValue := reflect.Indirect(reflect.ValueOf(data))
	switch reflectValue.Kind() {
	case reflect.Slice:
		for i := 0; i < reflectValue.Len(); i++ {
			var tmp = new(insertRequest)
			tmp = tmp.setColumns(reflectValue.Index(i).Interface())
			if len(pks) > 0 {
				tmp.pkValue = pks[0][i]
			}
			c.id = append(c.id, tmp)
		}

	default:
		c.err = errors.New("set_put_data的data只支持[]map[string]interface{}和[]struct")
	}

	return c
}

func (c *writeRowsRequest) SetUpdateData(data interface{}, pks ...[]int64) *writeRowsRequest {
	reflectValue := reflect.Indirect(reflect.ValueOf(data))
	switch reflectValue.Kind() {
	case reflect.Slice:
		for i := 0; i < reflectValue.Len(); i++ {
			var tmp = new(updateRequest)
			tmp = tmp.setColumns(reflectValue.Index(i).Interface())
			if len(pks) > 0 {
				tmp.pkValue = pks[0][i]
			}
			c.ud = append(c.ud, tmp)
		}

	default:
		c.err = errors.New("set_put_data的data只支持[]map[string]interface{}和[]struct")
	}

	return c
}

func (c *writeRowsRequest) SetDelData(pks []int64) *writeRowsRequest {
	for _, item := range pks {
		var dr = new(deleteRequest)
		dr.pkValue = item
		c.dd = append(c.dd, dr)
	}
	return c
}

func (c *writeRowsRequest) Do() (*int32, error) {
	var err error
	if c.err != nil {
		return nil, err
	}

	if c.dwSearchOrm.err != nil {
		return nil, c.dwSearchOrm.err
	}

	if len(c.ud) == 0 && len(c.id) == 0 && len(c.dd) == 0 {
		return nil, errors.New("没有传入要写入的数据")
	}

	batchWriteReq := &tablestore.BatchWriteRowRequest{}
	if len(c.id) > 0 {
		for _, row := range c.id {
			if row.err != nil {
				return nil, err
			}
			putRowChange := new(tablestore.PutRowChange)
			putRowChange.TableName = c.tableName
			putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)

			// pk处理
			putPk := new(tablestore.PrimaryKey)
			putPk.AddPrimaryKeyColumn(pkName, row.pkValue)

			// columns处理
			err = addPutRowColumn(row.columns, putRowChange)
			if err != nil {
				return nil, err
			}

			putRowChange.PrimaryKey = putPk
			batchWriteReq.AddRowChange(putRowChange)
		}
	}
	if len(c.ud) > 0 {
		for _, row := range c.ud {
			if row.err != nil {
				return nil, err
			}
			updateRowChange := new(tablestore.UpdateRowChange)
			updateRowChange.TableName = c.tableName
			updateRowChange.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)

			updatePk := new(tablestore.PrimaryKey)
			updatePk.AddPrimaryKeyColumn(pkName, row.pkValue)

			err = addUpdateRowColumn(row.columns, updateRowChange)
			if err != nil {
				return nil, err
			}

			updateRowChange.PrimaryKey = updatePk
			batchWriteReq.AddRowChange(updateRowChange)
		}
	}
	if len(c.dd) > 0 {
		for _, row := range c.dd {
			if row.err != nil {
				return nil, err
			}
			deleteRowChange := new(tablestore.DeleteRowChange)
			deleteRowChange.TableName = c.tableName
			deleteRowChange.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)

			deletePk := new(tablestore.PrimaryKey)
			deletePk.AddPrimaryKeyColumn(pkName, row.pkValue)

			deleteRowChange.PrimaryKey = deletePk
			batchWriteReq.AddRowChange(deleteRowChange)
		}
	}

	rows, ok := batchWriteReq.RowChangesGroupByTable[c.tableName]
	if !ok {
		return nil, errors.New("没有传入要写入的数据")
	}

	if len(rows) > otsLimitPutSize {
		// 按ots最大size拆分
		mapRows := splitRows(rows, otsLimitPutSize)
		for _, sRows := range mapRows {
			batchWriteReq.RowChangesGroupByTable[c.tableName] = sRows
			response, err := c.dwSearchOrm.client.BatchWriteRow(batchWriteReq)
			if err != nil {
				errStrBool := strings.Contains(err.Error(), postLimitSizeDoc)
				if errStrBool {
					retryBatchOtsOrder(c.dwSearchOrm.client, batchWriteReq, c.tableName)
				} else {
					fmt.Printf("BatchWriteRow batch request failed with: %v, %s, count: %d", response, err.Error(), len(sRows))
					continue
				}
			} else {
				//fmt.Printf("BatchWriteRow batch request success count: %d条", len(rows))
			}
		}
	} else {
		response, err := c.dwSearchOrm.client.BatchWriteRow(batchWriteReq)
		if err != nil {
			errStrBool := strings.Contains(err.Error(), postLimitSizeDoc)
			if errStrBool {
				retryBatchOtsOrder(c.dwSearchOrm.client, batchWriteReq, c.tableName)
			} else {
				fmt.Printf("BatchWriteRow batch request failed with: %v, %s, count: %d条", response, err.Error(), len(rows))
			}
		} else {
			//fmt.Printf("BatchWriteRow batch request success count: %d条", len(rows))
		}
	}
	changeRows := int32(len(rows))
	return &changeRows, nil
}
