package gots

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
)

func (c *searchRowsRequest) setOrder() []search.Sorter {
	var sorters []search.Sorter
	if len(c.order) > 0 {
		for _, sort := range c.order {
			switch sort.filedName {
			case DirectionScoreSort:
				var sorter = new(search.ScoreSort)
				if sort.direction == directionForward {
					sorter.Order = search.SortOrder_ASC.Enum()
				} else {
					sorter.Order = search.SortOrder_DESC.Enum()
				}
				sorters = append(sorters, sorter)
			case DirectionPkSort:
				var sorter = new(search.PrimaryKeySort)
				if sort.direction == directionForward {
					sorter.Order = search.SortOrder_ASC.Enum()
				} else {
					sorter.Order = search.SortOrder_DESC.Enum()
				}
				sorters = append(sorters, sorter)
			default:
				var sorter = new(search.FieldSort)
				sorter.FieldName = sort.filedName
				if sort.direction == directionForward {
					sorter.Order = search.SortOrder_ASC.Enum()
				} else {
					sorter.Order = search.SortOrder_DESC.Enum()
				}
				sorters = append(sorters, sorter)
			}
		}
	} else {
		var sorter = new(search.PrimaryKeySort)
		sorter.Order = search.SortOrder_DESC.Enum()
		sorters = append(sorters, sorter)
	}
	return sorters
}

func (c *searchRowsRequest) setPageLimit() int32 {
	if c.limit == 0 {
		return 10
	} else {
		return c.limit
	}
}
