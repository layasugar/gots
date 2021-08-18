package gots

import (
	"errors"
	"reflect"
)

func scan(rows IRow, elem reflect.Value) error {
	pks, cols, ok := rows.Next()
	if !ok {
		return errors.New("empty rows")
	}

	if elem.Kind() == reflect.Map && elem.IsNil() {
		elem.Set(reflect.MakeMap(elem.Type()))
	}

	decoder, err := NewDecoder(elem.Kind())
	if err != nil {
		return err
	}

	return decoder.Decode(pks, cols, elem)
}

// ScanRows
func Scan(rows IRow, objs interface{}) error {
	rows.Reset()
	if rows.Len() == 0 {
		return nil
	}

	elems := reflect.Indirect(reflect.ValueOf(objs))
	if kind := elems.Kind(); kind != reflect.Slice {
		return scan(rows, elems)
	}
	elems.Set(reflect.MakeSlice(elems.Type(), 0, rows.Len()))

	var isPtr, isMap bool
	elemtype := elems.Type().Elem() // 获取Slice元素的类型
	switch elemtype.Kind() {
	case reflect.Ptr:
		isPtr = true
		elemtype = elemtype.Elem()
	case reflect.Map:
		isMap = true
	}

	decoder, err := NewDecoder(elemtype.Kind())
	if err != nil {
		return err
	}

	for i := 0; i < rows.Len(); i++ {
		pks, cols, ok := rows.Next()
		if !ok {
			continue
		}

		var dest reflect.Value
		if isMap {
			dest = reflect.MakeMap(elemtype)
		} else {
			dest = reflect.New(elemtype).Elem()
		}

		if err = decoder.Decode(pks, cols, dest); err != nil {
			break
		}

		if isPtr {
			dest = dest.Addr()
		}

		elems.Set(reflect.Append(elems, dest))
	}

	return err
}

type Decoder interface {
	Decode(PrimaryKeyCols, AttributeCols, reflect.Value) error
}

type MapDecoder struct{}

func (d MapDecoder) Decode(pks PrimaryKeyCols, cols AttributeCols, elem reflect.Value) error {
	elem = reflect.Indirect(elem)

	for _, col := range pks {
		elem.SetMapIndex(reflect.ValueOf(col.ColumnName),
			reflect.ValueOf(col.Value))
	}

	for _, col := range cols {
		elem.SetMapIndex(reflect.ValueOf(col.ColumnName),
			reflect.ValueOf(col.Value))
	}
	return nil
}

type StructDecoder struct{}

func (d StructDecoder) decode(data map[string]interface{}, elem reflect.Value) error {
	t := elem.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		switch field.Type.Kind() {
		case reflect.Struct:
			d.decode(data, elem.Field(i))
		case reflect.Ptr:
			if elem.Field(i).IsNil() {
				elem.Field(i).Set(reflect.New(field.Type.Elem()))
			}
			d.decode(data, elem.Field(i).Elem())
		default:
			if key := field.Tag.Get("json"); key != "" {
				if v, has := data[key]; has {
					elem.Field(i).Set(reflect.ValueOf(v))
				}
			}
		}
	}

	return nil
}

func (d StructDecoder) Decode(pks PrimaryKeyCols, cols AttributeCols, elem reflect.Value) error {
	data := make(map[string]interface{})

	for _, pk := range pks {
		data[pk.ColumnName] = pk.Value
	}

	for _, col := range cols {
		data[col.ColumnName] = col.Value
	}

	elem = reflect.Indirect(elem)
	return d.decode(data, elem)
}

func NewDecoder(kind reflect.Kind) (Decoder, error) {
	switch kind {
	case reflect.Map:
		return MapDecoder{}, nil
	case reflect.Struct:
		return StructDecoder{}, nil
	default:
		return nil, errors.New("unexpect error")
	}
}
