package gots

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

// 主键范围查询多条
func (c *tableStoreClient) GetRange(start, end int64) *getRangeRowsRequest {
	var ir = &getRangeRowsRequest{
		dwSearchOrm: c.dwSearchOrm,
		tableName:   c.tableName,
		startValue:  start,
		endValue:    end,
	}

	return ir
}

func (c *getRangeRowsRequest) Fields(columns []string) *getRangeRowsRequest {
	c.columns = columns
	return c
}

func (c *getRangeRowsRequest) Order(direction string) *getRangeRowsRequest {
	c.direction = direction
	return c
}

func (c *getRangeRowsRequest) Limit(limit int32) *getRangeRowsRequest {
	c.limit = limit
	return c
}

func (c *getRangeRowsRequest) Do() (*tablestore.GetRangeResponse, error) {
	var err error
	if c.err != nil {
		return nil, err
	}

	if c.dwSearchOrm.err != nil {
		return nil, c.dwSearchOrm.err
	}

	if c.limit == 0 {
		c.limit = 10
	}

	if c.direction == "" {
		c.direction = directionForward
	}

	if c.startValue == 0 {
		return nil, errors.New("pk开始值不能为空")
	}

	if c.endValue == 0 {
		return nil, errors.New("pk结束值不能为空")
	}

	getRangeRequest := &tablestore.GetRangeRequest{}
	rangeRowQueryCriteria := &tablestore.RangeRowQueryCriteria{}
	rangeRowQueryCriteria.TableName = c.tableName
	rangeRowQueryCriteria.MaxVersion = 1
	rangeRowQueryCriteria.Limit = c.limit

	if c.direction == directionForward {
		rangeRowQueryCriteria.Direction = tablestore.FORWARD
	} else {
		rangeRowQueryCriteria.Direction = tablestore.BACKWARD
	}

	startPK := new(tablestore.PrimaryKey)
	startPK.AddPrimaryKeyColumn(pkName, c.startValue)
	rangeRowQueryCriteria.StartPrimaryKey = startPK
	endPK := new(tablestore.PrimaryKey)
	endPK.AddPrimaryKeyColumn(pkName, c.endValue)
	rangeRowQueryCriteria.EndPrimaryKey = endPK

	if len(c.columns) > 0 {
		rangeRowQueryCriteria.ColumnsToGet = c.columns
	}

	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria

	res, err := c.dwSearchOrm.client.GetRange(getRangeRequest)
	if err != nil {
		return nil, err
	}

	if res == nil || res.Rows == nil || len(res.Rows) == 0 {
		return nil, OtsNil
	}

	return res, nil
}

func (c *getRangeRowsRequest) Scan(objs interface{}) error {
	res, err := c.Do()
	if err != nil {
		return err
	}

	rows := &rangeGet{GetRangeResponse: res, cursor: 0}
	return Scan(rows, objs)
}
