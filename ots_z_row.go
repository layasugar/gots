package gots

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type (
	AttributeCols  = []*tablestore.AttributeColumn
	PrimaryKeyCols = []*tablestore.PrimaryKeyColumn
)

type IRow interface {
	Len() int
	Reset()
	Next() (PrimaryKeyCols, AttributeCols, bool)
}

func NewRows(resp interface{}) (rows IRow, err error) {
	switch resp.(type) {
	case *tablestore.GetRowResponse:
		r := resp.(*tablestore.GetRowResponse)
		rows = &rowGet{GetRowResponse: r, cursor: 0}
	case *tablestore.SearchResponse:
		r := resp.(*tablestore.SearchResponse)
		rows = &rowSearch{SearchResponse: r, cursor: 0}
	case *tablestore.GetRangeResponse:
		r := resp.(*tablestore.GetRangeResponse)
		rows = &rangeGet{GetRangeResponse: r, cursor: 0}
	case []tablestore.RowResult:
		r := resp.([]tablestore.RowResult)
		rows = &rowResults{rows: r, cursor: 0}
	case []Row:
		r := resp.([]Row)
		rows = &Rows{rows: r, len: len(r), cursor: 0}
	default:
		err = errors.New("unsupported response type")
	}
	return
}

func UnmarshalResp(resp interface{}, v interface{}) error {
	rows, err := NewRows(resp)
	if err != nil {
		return err
	}

	return Scan(rows, v)
}

type Row struct {
	PrimaryKeys PrimaryKeyCols
	Columns     AttributeCols
}

type Rows struct {
	len    int
	cursor int
	rows   []Row
}

func (rs *Rows) Len() int {
	return rs.len
}

func (rs *Rows) Reset() {
	rs.cursor = 0
}

func (rs *Rows) Next() (pks PrimaryKeyCols, cols AttributeCols, ok bool) {
	if rs.cursor >= rs.len {
		return
	}
	defer func() {
		rs.cursor++
	}()

	ok = true
	pks = rs.rows[rs.cursor].PrimaryKeys
	cols = rs.rows[rs.cursor].Columns
	return
}

type rowGet struct {
	*tablestore.GetRowResponse
	cursor int
}

func (r *rowGet) Len() int { return 1 }

func (r *rowGet) Reset() { r.cursor = 0 }

func (r *rowGet) Next() (pks PrimaryKeyCols, cols AttributeCols, ok bool) {
	if r.cursor > 0 {
		return
	}
	pks, cols, ok = r.PrimaryKey.PrimaryKeys, r.Columns, true
	r.cursor++
	return
}

type rowSearch struct {
	*tablestore.SearchResponse
	cursor int
}

func (r *rowSearch) Len() int {
	return len(r.Rows)
}

func (r *rowSearch) Reset() {
	r.cursor = 0
}

func (r *rowSearch) Next() (pks PrimaryKeyCols, cols AttributeCols, ok bool) {
	if r.cursor < len(r.Rows) {
		row := r.Rows[r.cursor]
		if row.PrimaryKey != nil {
			pks = row.PrimaryKey.PrimaryKeys
		}
		cols, ok = row.Columns, true
		r.cursor++
	}

	return
}

type rangeGet struct {
	*tablestore.GetRangeResponse
	cursor int
}

func (r *rangeGet) Len() int {
	return len(r.Rows)
}

func (r *rangeGet) Reset() {
	r.cursor = 0
}

func (r *rangeGet) Next() (pks PrimaryKeyCols, cols AttributeCols, ok bool) {
	if r.cursor < len(r.Rows) {
		row := r.Rows[r.cursor]
		if row.PrimaryKey != nil {
			pks = row.PrimaryKey.PrimaryKeys
		}
		cols, ok = row.Columns, true
		r.cursor++
	}

	return
}

type rowResults struct {
	rows   []tablestore.RowResult
	cursor int
}

func (r *rowResults) Len() int {
	return len(r.rows)
}

func (r *rowResults) Reset() {
	r.cursor = 0
}

func (r *rowResults) Next() (pks PrimaryKeyCols, cols AttributeCols, ok bool) {
	if r.cursor < len(r.rows) {
		row := r.rows[r.cursor]
		if row.IsSucceed {
			pks = row.PrimaryKey.PrimaryKeys
			cols, ok = row.Columns, true
		}
		r.cursor++
	}

	return
}
