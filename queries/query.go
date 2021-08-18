package queries

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
)

func TermQuery(field string, term interface{}) *search.TermQuery {
	query := &search.TermQuery{
		FieldName: field,
		Term:      term,
	}
	return query
}

func TermsQuery(field string, terms ...interface{}) *search.TermsQuery {
	query := &search.TermsQuery{
		FieldName: field,
		Terms:     terms,
	}
	return query
}

func RangeQuery(field string, operator string, value interface{}) *search.RangeQuery {
	query := &search.RangeQuery{
		FieldName: field,
	}

	switch operator {
	case ">":
		query.GT(value)
	case "<":
		query.LT(value)
	case ">=":
		query.GTE(value)
	case "<=":
		query.LTE(value)
	}

	return query
}

func MatchAllQuery() *search.MatchAllQuery {
	query := &search.MatchAllQuery{}
	return query
}

func MatchQuery(field, text string, minimumShouldMatch *int32, operator *search.QueryOperator) *search.MatchQuery {
	query := &search.MatchQuery{
		FieldName:          field,
		Text:               text,
		MinimumShouldMatch: minimumShouldMatch,
		Operator:           operator,
	}
	return query
}

func MatchPhraseQuery(field, text string) *search.MatchPhraseQuery {
	query := &search.MatchPhraseQuery{
		FieldName: field,
		Text:      text,
	}
	return query
}

func PrefixQuery(field, prefix string) *search.PrefixQuery {
	query := &search.PrefixQuery{
		FieldName: field,
		Prefix:    prefix,
	}
	return query
}

func WildcardQuery(field, value string) *search.WildcardQuery {
	query := &search.WildcardQuery{
		FieldName: field,
		Value:     value,
	}
	return query
}

func NestedQuery(path string, query search.Query, scoreMode search.ScoreModeType) *search.NestedQuery {
	q := &search.NestedQuery{
		Path:      path,
		Query:     query,
		ScoreMode: scoreMode,
	}
	return q
}
