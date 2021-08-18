package gots

var OtsNil = NilRes{}

type NilRes struct{}

func (n NilRes) Error() string {
	return "ots response data is null"
}
