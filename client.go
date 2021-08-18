package gots

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

func NewClient(options ...OptionFunc) *DwSearchOrm {
	var conf = new(otsClientConf)
	var client = new(DwSearchOrm)
	client.otsClientConf = conf
	for _, f := range options {
		f(client)
	}

	client.err = client.initOtsClient()
	return client
}

func SetEndPoint(endPoint string) OptionFunc {
	return func(orm *DwSearchOrm) {
		orm.otsClientConf.endPoint = endPoint
	}
}

func SetInstanceName(instanceName string) OptionFunc {
	return func(orm *DwSearchOrm) {
		orm.otsClientConf.instanceName = instanceName
	}
}

func SetAKI(aki string) OptionFunc {
	return func(orm *DwSearchOrm) {
		orm.otsClientConf.accessKeyID = aki
	}
}

func SetAKS(aks string) OptionFunc {
	return func(orm *DwSearchOrm) {
		orm.otsClientConf.accessKeySecret = aks
	}
}

func (c *DwSearchOrm) Table(table string) *tableStoreClient {
	var tsc = new(tableStoreClient)
	tsc.tableName = table
	tsc.dwSearchOrm = c
	return tsc
}

func (c *DwSearchOrm) DDL(table string) *ddlClient {
	var ds = new(ddlClient)
	ds.tableName = table
	ds.dwSearchOrm = c
	return ds
}

func (c *DwSearchOrm) initOtsClient() error {
	if c.otsClientConf.endPoint != "" &&
		c.otsClientConf.instanceName != "" &&
		c.otsClientConf.accessKeyID != "" &&
		c.otsClientConf.accessKeySecret != "" {
		c.client = tablestore.NewClient(c.otsClientConf.endPoint,
			c.otsClientConf.instanceName,
			c.otsClientConf.accessKeyID,
			c.otsClientConf.accessKeySecret)
	} else {
		return errors.New("ots配置不能为空")
	}
	return nil
}
