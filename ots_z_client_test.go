package gots

import (
	"encoding/json"
	"errors"
	"github.com/layasugar/otsorm-go/queries"
	"testing"
)

type myObject struct {
	Id       int64  `json:"pk1"`
	ComMet   string `json:"commet"`
	Garden   int64  `json:"garden"`
	Number   int64  `json:"number"`
	Phone    string `json:"phone"`
	Username string `json:"username"`
}

// 配置下参数，并创建名为test的表，执行完TestInsertRequest_Do，自动生成一下多元索引，已完成下面的单元测试
var Client = NewClient(
	SetEndPoint("xxx"),
	SetInstanceName("xxx"),
	SetAKI("xxx"),
	SetAKS("xxx"),
)

// 行存在会覆盖行(完全覆盖)，行不存在会写入行
func TestInsertRequest_Do(t *testing.T) {
	t.Log("开始测试struct添加")
	h := myObject{1, "测试结构体添加", 20, 12, "123456789101", "李四1"}
	resp, err := Client.Table("test").Insert(&h).Do()
	if err != nil {
		t.Log(err.Error())
		return
	}
	t.Logf("结构体增加行数: %d行", *resp)

	t.Log("开始测试map添加")
	resp, err = Client.Table("test").Insert(map[string]interface{}{
		"pk1":      2,
		"commet":   "测试map添加",
		"garden":   21,
		"number":   21,
		"phone":    "3213854513",
		"username": "李四2",
	}).Do()
	if err != nil {
		t.Log(err.Error())
		return
	}

	t.Logf("map增加行数: %d行", *resp)

	// 写入几条测试数据
	h3 := myObject{3, "测试结构体添加", 20, 12, "123456789101", "李四3"}
	h4 := myObject{4, "测试结构体添加", 20, 12, "123456789101", "李四4"}
	h5 := myObject{5, "测试结构体添加", 20, 12, "123456789101", "李四5"}
	h6 := myObject{6, "测试结构体添加", 20, 12, "123456789101", "李四6"}
	_, _ = Client.Table("test").Insert(&h3).Do()
	_, _ = Client.Table("test").Insert(&h4).Do()
	_, _ = Client.Table("test").Insert(&h5).Do()
	_, _ = Client.Table("test").Insert(&h6).Do()

	// 行存在会覆盖行测试
	h7 := myObject{Id: 5, ComMet: "测试结构体添加行存在会覆盖行"}
	_, _ = Client.Table("test").Insert(&h7).Do()
	_, _ = Client.Table("test").Insert(map[string]interface{}{
		"pk1":    6,
		"commet": "测试map添加行存在会覆盖行",
	}).Do()
}

// 行存在会覆盖行(完全覆盖)，行不存在会写入行
func TestUpdateRequest_Do(t *testing.T) {
	t.Log("开始测试struct修改一条")
	h := myObject{Id: 1, ComMet: "测试struct修改"}
	count, err := Client.Table("test").Update(&h).Do()
	if err != nil {
		t.Log(err.Error())
		return
	}
	t.Logf("修改行数: %d行", *count)

	t.Log("开始测试map修改一行")
	count, err = Client.Table("test").Update(map[string]interface{}{"commet": "测试map修改", "pk1": 2}).Do()
	if err != nil {
		t.Log(err.Error())
		return
	}
	t.Logf("修改行数: %d行", *count)

	h1 := myObject{Id: 7, ComMet: "测试struct修改行不存在会插入一行"}
	_, _ = Client.Table("test").Update(&h1).Do()
}

// delete 行存在删除行，行不存在会忽略错误返回成功行数1
func TestDeleteRequest_Do(t *testing.T) {
	t.Log("开始测试删除")
	resp, err := Client.Table("test").Delete(7).Do()
	if err != nil {
		t.Log(err.Error())
		return
	}
	t.Logf("删除行数: %d行", *resp)

	// 行不存在
	t.Log("开始测试删除行不存在")
	count, err := Client.Table("test").Delete(8).Do()
	if err != nil {
		t.Log(err.Error())
		return
	}
	t.Logf("删除行数: %d行", *count)
}

// putData 行存在会覆盖行，行不存在会写入行
// updateData 行存在会覆盖行，行不存在会写入行
// delete 行存在删除行，行不存在会忽略错误
func TestWriteRowsRequest_Do(t *testing.T) {
	t.Log("开始测试map批量添加，批量修改，批量删除")

	var puts1 = []map[string]interface{}{
		{"pk1": 1, "commet": "测试批量map添加行存在覆盖行", "garden": 20, "number": 12, "phone": "123456789101", "username": "李四1"},
		{"pk1": 11, "commet": "测试批量map添加行不存在插入行", "garden": 20, "number": 12, "phone": "123456789101", "username": "李四11"},
		{"pk1": 12, "commet": "测试批量map添加行不存在插入行", "garden": 20, "number": 12, "phone": "123456789101", "username": "李四12"},
	}
	var updates1 = []map[string]interface{}{
		{"pk1": 2, "commet": "测试批量map修改行存在覆盖行", "garden": 18},
		{"pk1": 13, "commet": "测试批量map修改行不存在插入行", "garden": 18},
	}
	resp, err := Client.Table("test").WriteRows().SetPutData(puts1).SetUpdateData(updates1).Do()
	if err != nil {
		t.Log(err.Error())
		return
	}
	t.Logf("map批量行数： %d", *resp)

	t.Log("开始测试struct批量添加，批量修改，批量删除")
	var puts = []myObject{
		{3, "测试批量struct添加行存在覆盖行", 20, 12, "123456789101", "李四3"},
		{14, "测试批量struct添加行不存在插入行", 20, 12, "123456789101", "李四101"},
		{15, "测试批量struct添加行不存在插入行", 20, 12, "123456789101", "李四102"},
	}
	var updates = []myObject{
		{Id: 4, ComMet: "测试批量struct修改行存在覆盖行"},
		{Id: 16, ComMet: "测试批量struct修改行不存在插入行"},
	}
	resp, err = Client.Table("test").WriteRows().SetPutData(&puts).SetUpdateData(&updates).Do()
	if err != nil {
		t.Log(err.Error())
		return
	}
	t.Logf("struct批量行数： %d", *resp)

	t.Log("开始测试struct批量添加，批量修改，批量删除")
	count, err := Client.Table("test").WriteRows().SetDelData([]int64{5, 6, 99}).Do()
	if err != nil {
		t.Log(err.Error())
		return
	}
	t.Logf("批量删除行数： %d", *count)
	// 写点测试数据
	var puts2 = []myObject{
		{100, "测试数据", 20, 12, "123456789101", "李四100"},
		{101, "测试数据", 20, 12, "123456789101", "李四101"},
		{102, "测试数据", 20, 12, "123456789101", "李四102"},
		{103, "测试数据", 20, 12, "123456789101", "李四103"},
		{104, "测试数据", 20, 12, "123456789101", "李四104"},
		{105, "测试数据", 20, 12, "123456789101", "李四105"},
		{106, "测试数据", 20, 12, "123456789101", "李四106"},
		{107, "测试数据", 20, 12, "123456789101", "李四107"},
		{108, "测试数据", 20, 12, "123456789101", "李四108"},
		{109, "测试数据", 20, 12, "123456789101", "李四109"},
		{110, "测试数据", 20, 12, "123456789101", "李四110"},
		{111, "测试数据", 20, 12, "123456789101", "李四111"},
		{112, "测试数据", 20, 12, "123456789101", "李四112"},
	}
	_, err = Client.Table("test").WriteRows().SetPutData(&puts2).Do()
}

func TestGetRequest_Do(t *testing.T) {
	t.Log("开始测试主键获取一条")
	resp, err := Client.Table("test").Get(1).Do()
	if err != nil {
		if errors.Is(err, OtsNil) {
			t.Log("数据不存在")
			return
		}
		t.Logf("查询出错 err: %v", err)
		return
	}
	str, _ := json.Marshal(resp)
	t.Log(string(str))

	// scan struct
	var obj myObject
	err = Client.Table("test").Get(1).Scan(&obj)
	if err != nil {
		if errors.Is(err, OtsNil) {
			t.Log("数据不存在")
			return
		}
		t.Logf("查询出错 err: %v", err)
		return
	}
	t.Log(obj)
}

func TestGetRangeRowsRequest_Do(t *testing.T) {
	t.Log("开始测试主键范围获取")
	resp, err := Client.Table("test").GetRange(1, 4).Do()
	if err != nil {
		if errors.Is(err, OtsNil) {
			t.Log("数据不存在")
			return
		}
		t.Logf("查询出错 err: %v", err)
		return
	}
	str, _ := json.Marshal(resp)
	t.Log(string(str))

	var objs []myObject
	err = Client.Table("test").GetRange(1, 4).Scan(&objs)
	if err != nil {
		if errors.Is(err, OtsNil) {
			t.Log("数据不存在")
			return
		}
		t.Logf("查询出错 err: %v", err)
		return
	}
	t.Log(objs)
}

func TestGetRowsRequest_Do(t *testing.T) {
	t.Log("开始测试主键多条获取")
	resp, err := Client.Table("test").GetRows([]int64{1, 2, 3, 11}).Do()
	if err != nil {
		if errors.Is(err, OtsNil) {
			t.Log("数据不存在")
			return
		}
		t.Logf("查询出错 err: %v", err)
		return
	}
	str, _ := json.Marshal(resp)
	t.Log(string(str))

	var objs []myObject
	err = Client.Table("test").GetRows([]int64{1, 2, 3, 11}).Scan(&objs)
	if err != nil {
		if errors.Is(err, OtsNil) {
			t.Log("数据不存在")
			return
		}
		t.Logf("查询出错 err: %v", err)
		return
	}
	t.Log(objs)
}

// 单表各种条件查询
// 查询的列并没有记录，需要每次查询都传入，或者不传就是获取所有
func TestSearchRequest_Do(t *testing.T) {
	t.Log("查询一下username是李四1")
	q1 := queries.TermQuery("username", "李四1")
	var r1 []myObject
	err := Client.Table("test").Search("index_x1").Query(q1).Scan(&r1)
	if err != nil {
		if errors.Is(err, OtsNil) {
			t.Log("数据不存在")
			return
		}
		t.Logf("查询出错 err: %v", err)
		return
	}
	t.Log(r1)

	//
	t.Log("翻页使用")
	q2 := queries.TermQuery("garden", 20)
	searchRequest := Client.Table("test").Search("index_x1").Query(q2).Limit(10)
	searchResponse, err := searchRequest.Do()
	if err != nil {
		t.Log(err.Error())
		return
	}
	t.Log("IsAllSuccess: ", searchResponse.IsAllSuccess) //查看返回结果是否完整。
	t.Log("TotalCount: ", searchResponse.TotalCount)     //匹配的总行数。
	t.Log("RowCount: ", len(searchResponse.Rows))        //返回的行数。

	if len(searchResponse.Rows) > 0 {
		// todo your code
		var r2 []myObject
		rows, _ := NewRows(searchResponse)
		Scan(rows, &r2)
		t.Log(r2)
	}

	for searchResponse.NextToken != nil {
		searchResponse, err = searchRequest.Next(searchResponse.NextToken)
		if err != nil {
			t.Logf("search err: %s", err.Error())
			return
		}

		if len(searchResponse.Rows) > 0 {
			// todo your code
			var r2 []myObject
			rows, _ := NewRows(searchResponse)
			Scan(rows, &r2)
			t.Log(r2)
		}
	}

}
