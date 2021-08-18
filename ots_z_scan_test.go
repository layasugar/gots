package gots

import (
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type People struct {
	Name string `ots:"name"`
	Age  int    `ots:"age"`
}

type Student struct {
	*People
	No string `ots:"no"`
}

func TestScanRow(t *testing.T) {
	rows := Rows{
		len:    2,
		cursor: 0,
		rows: []Row{
			{
				PrimaryKeys: nil,
				Columns: []*tablestore.AttributeColumn{
					{ColumnName: "name", Value: "yuzj"},
					{ColumnName: "age", Value: 28},
					{ColumnName: "no", Value: "1"},
				},
			}, {
				PrimaryKeys: nil,
				Columns: []*tablestore.AttributeColumn{
					{ColumnName: "name", Value: "zhouh"},
					{ColumnName: "age", Value: 23},
					{ColumnName: "no", Value: "2"},
				},
			},
		},
	}

	var obj Student
	err := Scan(&rows, &obj)
	if err != nil {
		t.Error(err)
	}

	t.Log("Scan to Struct: ", obj.People, obj.No)

	var objs []Student
	err = Scan(&rows, &objs)
	if err != nil {
		t.Error(err)
	}

	for i := range objs {
		t.Logf("Scan to Slice[%d]: %v, %v", i, objs[i].People, objs[i].No)
	}

	var dic map[string]interface{}
	err = Scan(&rows, &dic)
	if err != nil {
		t.Error(err)
	}
	t.Log("Scan to map: ", dic)

	var dics []map[string]interface{}
	err = Scan(&rows, &dics)
	if err != nil {
		t.Error(err)
	}
	for i := range dics {
		t.Logf("Scan to MapSlice[%d]: %v", i, dics[i])
	}
}
