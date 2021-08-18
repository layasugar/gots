package gots

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"github.com/layasugar/otsorm-go/queries"
	"strings"
)

// 查询搜索分页
func (c *tableStoreClient) Search(indexName ...string) *searchRowsRequest {
	var ir = &searchRowsRequest{
		dwSearchOrm: c.dwSearchOrm,
		tableName:   c.tableName,
		foreign:     make(map[string]*SearchForeign),
	}
	if len(indexName) > 0 {
		ir.indexName = indexName[0]
	} else {
		ir.indexName = c.tableName + "_index"
	}

	return ir
}

func (c *searchRowsRequest) OrderBy(direction string) *searchRowsRequest {
	if direction != "" {
		orders := strings.Split(direction, ",")
		for _, item := range orders {
			fields := strings.Split(item, " ")
			if len(fields) > 0 {
				tmp := order{
					filedName: fields[0],
				}
				if len(fields) == 1 {
					tmp.direction = directionForward
				} else {
					tmp.direction = fields[1]
				}
				c.order = append(c.order, &tmp)
			}
		}
	}
	return c
}

func (c *searchRowsRequest) Limit(limit int32) *searchRowsRequest {
	if limit > 100 {
		c.err = errors.New("limit最大值为100")
	}
	c.limit = limit
	return c
}

func (c *searchRowsRequest) Offset(offset int32) *searchRowsRequest {
	c.offset = offset
	return c
}

func (c *searchRowsRequest) Next(nextToken []byte) (*tablestore.SearchResponse, error) {
	c.nextToken = nextToken
	return c.Do()
}

func (c *searchRowsRequest) Fields(columns []string) *searchRowsRequest {
	c.columns = columns
	return c
}

func (c *searchRowsRequest) Query(where ...search.Query) *searchRowsRequest {
	if len(where) > 1 {
		c.query = queries.And(where...)
	} else if len(where) == 1 {
		c.query = where[0]
	} else {
		c.query = queries.MatchAllQuery()
	}
	return c
}

func (c *searchRowsRequest) setForeign(foreign Foreign, query ...search.Query) {
	if len(query) > 0 {
		hashForeignKey := getHashKey(foreign.ForeignTable, foreign.ForeignKey, foreign.JoinForeignKey, foreign.References)
		if item, ok := c.foreign[hashForeignKey]; ok {
			item.query = queries.And(item.query, queries.And(query...))
		} else {
			c.foreign[hashForeignKey] = &SearchForeign{foreign: foreign, query: queries.And(query...)}
		}
	} else {
		hashForeignKey := getHashKey(foreign.ForeignTable, foreign.ForeignKey, foreign.JoinForeignKey, foreign.References)
		if _, ok := c.foreign[hashForeignKey]; !ok {
			c.foreign[hashForeignKey] = &SearchForeign{foreign: foreign, query: nil}
		}
	}
}

func (c *searchRowsRequest) Preloads(foreign ...Foreign) *searchRowsRequest {
	if len(foreign) > 0 {
		for _, item := range foreign {
			c.setForeign(item)
		}
	}
	return c
}

func (c *searchRowsRequest) Do() (*tablestore.SearchResponse, error) {
	var err error
	if c.err != nil {
		return nil, err
	}

	if c.dwSearchOrm.err != nil {
		return nil, c.dwSearchOrm.err
	}

	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(c.tableName)
	searchRequest.SetIndexName(c.indexName)

	searchColumns := &tablestore.ColumnsToGet{}
	if len(c.columns) > 0 {
		searchColumns.Columns = c.columns
		searchColumns.ReturnAll = false
	} else {
		searchColumns.ReturnAll = true
	}
	searchRequest.SetColumnsToGet(searchColumns)

	searchQuery := search.NewSearchQuery()
	searchQuery.SetGetTotalCount(true)
	if len(c.nextToken) > 0 {
		searchQuery.SetToken(c.nextToken)
	} else {
		if c.offset+c.limit > 10000 {
			return nil, errors.New("限制翻页数据总页码+limit不得大于10000")
		}
		// 设置翻页参数
		searchQuery.SetOffset(c.offset)
		searchQuery.SetLimit(c.setPageLimit())

		// 设置排序规则
		searchQuery.SetSort(&search.Sort{Sorters: c.setOrder()})

		// 设置查询条件
		searchQuery.SetQuery(c.query)
	}
	searchRequest.SetSearchQuery(searchQuery)

	res, err := c.dwSearchOrm.client.Search(searchRequest)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *searchRowsRequest) Scan(objs interface{}) error {
	res, err := c.Do()
	if err != nil {
		return err
	}

	rows := &rowSearch{SearchResponse: res, cursor: 0}
	return Scan(rows, objs)
}
