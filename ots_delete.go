package gots

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

// 主键删除1条
func (c *tableStoreClient) Delete(pkValue int64) *deleteRequest {
	var ir = &deleteRequest{
		dwSearchOrm: c.dwSearchOrm,
		tableName:   c.tableName,
	}
	ir.pkValue = pkValue

	return ir
}

// 返回受影响的行数和error
func (c *deleteRequest) Do() (*int32, error) {
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

	deleteRowReq := new(tablestore.DeleteRowRequest)
	deleteRowReq.DeleteRowChange = new(tablestore.DeleteRowChange)
	deleteRowReq.DeleteRowChange.TableName = c.tableName
	deleteRowReq.DeleteRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)

	deletePk := new(tablestore.PrimaryKey)
	deletePk.AddPrimaryKeyColumn(pkName, c.pkValue)

	deleteRowReq.DeleteRowChange.PrimaryKey = deletePk
	resp, err := c.dwSearchOrm.client.DeleteRow(deleteRowReq)
	if err != nil {
		return nil, err
	}

	var counts int32
	if resp != nil && resp.ConsumedCapacityUnit != nil {
		counts = resp.ConsumedCapacityUnit.Write
	}
	return &counts, nil
}
