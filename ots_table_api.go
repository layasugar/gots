package gots

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"
)

func (c *ddlClient) DelTable() error {
	if c.dwSearchOrm.err != nil {
		return c.dwSearchOrm.err
	}

	deleteReq := new(tablestore.DeleteTableRequest)
	deleteReq.TableName = c.tableName
	_, err := c.dwSearchOrm.client.DeleteTable(deleteReq)
	return err
}

func (c *ddlClient) DeleteSearchIndex(indexName string) error {
	if c.dwSearchOrm.err != nil {
		return c.dwSearchOrm.err
	}

	request := &tablestore.DeleteSearchIndexRequest{}
	request.TableName = c.tableName                           //设置数据表名称
	request.IndexName = indexName                             //设置多元索引名称
	_, err := c.dwSearchOrm.client.DeleteSearchIndex(request) //调用client删除多元索引
	return err
}

func (c *ddlClient) DeleteTunnel(tunnelName string) error {
	tunnelClient := tunnel.NewTunnelClient(c.dwSearchOrm.otsClientConf.endPoint, c.dwSearchOrm.otsClientConf.instanceName,
		c.dwSearchOrm.otsClientConf.accessKeyID, c.dwSearchOrm.otsClientConf.accessKeySecret)

	req := &tunnel.DeleteTunnelRequest{
		TableName:  c.tableName,
		TunnelName: tunnelName,
	}
	_, err := tunnelClient.DeleteTunnel(req)
	return err
}

func (c *ddlClient) CreateTable() error {
	if c.dwSearchOrm.err != nil {
		return c.dwSearchOrm.err
	}

	createTableRequest := new(tablestore.CreateTableRequest)
	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = c.tableName
	tableMeta.AddPrimaryKeyColumn("pk1", tablestore.PrimaryKeyType_INTEGER)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	reservedThroughput := new(tablestore.ReservedThroughput)
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput

	_, err := c.dwSearchOrm.client.CreateTable(createTableRequest)
	return err
}

func (c *ddlClient) CreateSearchIndex(indexName string, index []*tablestore.FieldSchema) error {
	request := &tablestore.CreateSearchIndexRequest{}
	request.TableName = c.tableName //设置数据表名称
	request.IndexName = indexName   //设置多元索引名称

	if len(index) == 0 {
		return nil
	}

	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: index, //设置多元索引包含的字段。
	}
	_, err := c.dwSearchOrm.client.CreateSearchIndex(request) //调用client创建多元索引。
	return err
}

func (c *ddlClient) CreateTunnel(tunnelName string) error {
	tunnelClient := tunnel.NewTunnelClient(c.dwSearchOrm.otsClientConf.endPoint, c.dwSearchOrm.otsClientConf.instanceName,
		c.dwSearchOrm.otsClientConf.accessKeyID, c.dwSearchOrm.otsClientConf.accessKeySecret)

	req := &tunnel.CreateTunnelRequest{
		TableName:  c.tableName,
		TunnelName: tunnelName,
		Type:       tunnel.TunnelTypeBaseStream, //创建全量加增量类型的Tunnel
	}
	_, err := tunnelClient.CreateTunnel(req)
	return err
}
