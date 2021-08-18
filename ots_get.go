package gots

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

// 主键查询1条
func (c *tableStoreClient) Get(pkValue int64) *getRequest {
	var ir = &getRequest{
		dwSearchOrm: c.dwSearchOrm,
		tableName:   c.tableName,
		pkValue:     pkValue,
	}

	return ir
}

func (c *getRequest) Fields(columns []string) *getRequest {
	c.columns = columns
	return c
}

func (c *getRequest) Do() (*tablestore.GetRowResponse, error) {
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

	getRowRequest := new(tablestore.GetRowRequest)
	criteria := new(tablestore.SingleRowQueryCriteria)
	putPk := new(tablestore.PrimaryKey)
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = c.tableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1
	putPk.AddPrimaryKeyColumn(pkName, c.pkValue)

	if len(c.columns) > 0 {
		getRowRequest.SingleRowQueryCriteria.ColumnsToGet = c.columns
	}

	criteria.PrimaryKey = putPk
	res, err := c.dwSearchOrm.client.GetRow(getRowRequest)
	if err != nil {
		return nil, err
	}

	if res == nil || res.Columns == nil || len(res.Columns) == 0 {
		return nil, OtsNil
	}
	return res, nil
}

func (c *getRequest) Scan(objs interface{}) error {
	res, err := c.Do()
	if err != nil {
		return err
	}

	rows := &rowGet{GetRowResponse: res, cursor: 0}

	return Scan(rows, objs)
}
