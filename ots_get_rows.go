package gots

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

// 主键查询多条
func (c *tableStoreClient) GetRows(pks []int64) *getRowsRequest {
	var ir = &getRowsRequest{
		dwSearchOrm: c.dwSearchOrm,
		tableName:   c.tableName,
		pks:         pks,
	}

	return ir
}

func (c *getRowsRequest) Fields(columns []string) *getRowsRequest {
	c.columns = columns
	return c
}

func (c *getRowsRequest) Do() ([]tablestore.RowResult, error) {
	var err error
	if c.err != nil {
		return nil, err
	}

	if c.dwSearchOrm.err != nil {
		return nil, c.dwSearchOrm.err
	}

	if len(c.pks) < 1 {
		return nil, errors.New("pk是必须的")
	}

	batchGetReq := &tablestore.BatchGetRowRequest{}
	mqCriteria := &tablestore.MultiRowQueryCriteria{}

	for _, pkValue := range c.pks {
		pkToGet := new(tablestore.PrimaryKey)
		pkToGet.AddPrimaryKeyColumn(pkName, pkValue)
		mqCriteria.AddRow(pkToGet)
		mqCriteria.MaxVersion = 1
	}

	if len(c.columns) > 0 {
		mqCriteria.ColumnsToGet = c.columns
	}

	mqCriteria.TableName = c.tableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	res, err := c.dwSearchOrm.client.BatchGetRow(batchGetReq)
	if err != nil {
		return nil, err
	}
	if res == nil || res.TableToRowsResult == nil || len(res.TableToRowsResult) == 0 || len(res.TableToRowsResult[c.tableName]) == 0 {
		return nil, OtsNil
	}

	return res.TableToRowsResult[c.tableName], nil
}

func (c *getRowsRequest) Scan(objs interface{}) error {
	res, err := c.Do()
	if err != nil {
		return err
	}

	rows := &rowResults{rows: res, cursor: 0}
	return Scan(rows, objs)
}
