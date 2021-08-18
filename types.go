package gots

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
)

const (
	otsLimitGetSize     = 100 // 限制单次获取得最大数量
	otsLimitPutSize     = 200 // 单次最大200条
	postLimitSizeDoc    = "OTSParameterInvalid The total data size of BatchWriteRow request exceeds the limit, limit size: 4194304,"
	pkTypeAutoIncrement = "auto"
	pkTypeMin           = "min"
	pkTypeMax           = "max"
	directionForward    = "asc"  // 正序
	directionBackward   = "desc" // 倒序
	DirectionScoreSort  = "ots_score_sort"
	DirectionPkSort     = "ots_pk_sort"
	pkName              = "pk1"
)

type OptionFunc func(orm *DwSearchOrm)

type columns = []*tablestore.AttributeColumn

type DwSearchOrm struct {
	client        *tablestore.TableStoreClient
	otsClientConf *otsClientConf
	err           error
}

type otsClientConf struct {
	endPoint        string
	instanceName    string
	accessKeyID     string
	accessKeySecret string
}

type attributeColumn struct {
	columnName string
	value      interface{}
}

type tableStoreClient struct {
	dwSearchOrm *DwSearchOrm
	tableName   string
}

type ddlClient struct {
	dwSearchOrm *DwSearchOrm
	tableName   string
}

type insertRequest struct {
	tableName   string
	pkValue     int64
	columns     []attributeColumn
	dwSearchOrm *DwSearchOrm
	err         error
}

type deleteRequest struct {
	tableName   string
	pkValue     int64
	dwSearchOrm *DwSearchOrm
	err         error
}

type updateRequest struct {
	tableName   string
	pkValue     int64
	columns     []attributeColumn
	dwSearchOrm *DwSearchOrm
	err         error
}

type getRequest struct {
	tableName   string
	pkValue     int64
	columns     []string
	dwSearchOrm *DwSearchOrm
	err         error
}

type getRowsRequest struct {
	tableName   string
	pks         []int64
	columns     []string
	dwSearchOrm *DwSearchOrm
	err         error
}

type getRangeRowsRequest struct {
	tableName   string
	limit       int32
	direction   string
	startValue  int64
	endValue    int64
	columns     []string
	dwSearchOrm *DwSearchOrm
	err         error
}

type writeRowsRequest struct {
	tableName   string
	dwSearchOrm *DwSearchOrm
	err         error
	ud          []*updateRequest
	id          []*insertRequest
	dd          []*deleteRequest
}

type Query = searchRowsRequest

type searchRowsRequest struct {
	tableName   string
	indexName   string
	columns     []string
	offset      int32
	limit       int32
	order       []*order
	nextToken   []byte
	query       search.Query
	dwSearchOrm *DwSearchOrm
	foreign     map[string]*SearchForeign
	err         error
}

type order struct {
	filedName string
	direction string
}

type Foreign struct {
	ForeignTable     string   // 关联表名称
	ForeignKey       string   // 当前表字段
	JoinForeignKey   string   // 关联表字段
	ForeignIndexName string   // 关联表索引名称
	References       string   // 数据key
	HasMany          bool     // 是否1对多
	Columns          []string // 要加载的数据列
}

type SearchForeign struct {
	foreign Foreign
	query   search.Query
}

type Join func()

type searchRowsResp struct {
}
