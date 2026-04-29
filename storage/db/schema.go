package db

import (
	"encoding/binary"
	"fmt"
)

type ColType uint8

const (
	TypeInt  ColType = 1
	TypeText ColType = 2
)

type Column struct {
	Name string
	Type ColType
}

type Schema struct {
	Columns         []Column
	PrimaryKeyIndex int
}

// Value is the closed set of column value types. The unexported method
// prevents external packages from adding new types.
type Value interface {
	encode(dst []byte) []byte
}

type (
	IntValue  uint64
	TextValue string
)

func (v IntValue) encode(dst []byte) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(v))
}

func (v TextValue) encode(dst []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(v)))
	return append(dst, v...)
}

func decodeValue(buf []byte, t ColType) (Value, int, error) {
	switch t {
	case TypeInt:
		if len(buf) < 8 {
			return nil, 0, fmt.Errorf("decode int: need 8 bytes, have %d", len(buf))
		}
		n := binary.BigEndian.Uint64(buf[:8])
		return IntValue(n), 8, nil

	case TypeText:
		if len(buf) < 4 {
			return nil, 0, fmt.Errorf("decode text length: need 4 bytes, have %d", len(buf))
		}
		length := binary.BigEndian.Uint32(buf[:4])
		if len(buf) < 4+int(length) {
			return nil, 0, fmt.Errorf("decode text body: length %d but only %d bytes available", length, len(buf)-4)
		}
		return TextValue(buf[4 : 4+length]), 4 + int(length), nil

	default:
		return nil, 0, fmt.Errorf("decode value: unknown type %d", t)
	}
}

type Row []Value

func (s *Schema) decodeRow(buf []byte) (Row, error) {
	row := make(Row, 0, len(s.Columns))
	offset := 0
	for _, column := range s.Columns {
		val, n, err := decodeValue(buf[offset:], column.Type)
		if err != nil {
			return nil, err
		}
		offset += n
		row = append(row, val)
	}

	if offset != len(buf) {
		return nil, fmt.Errorf("decode row: %v trailing bytes after consuming %v", len(buf)-offset, offset)
	}
	return row, nil
}

func (s *Schema) validateRow(row Row) error {
	if len(row) != len(s.Columns) {
		return fmt.Errorf("validate row: got %d values, schema has %d columns", len(row), len(s.Columns))
	}
	for i, column := range s.Columns {
		switch column.Type {
		case TypeInt:
			if _, ok := row[i].(IntValue); !ok {
				return fmt.Errorf("validate row: column %d (%q) expects int, got %T", i, column.Name, row[i])
			}
		case TypeText:
			if _, ok := row[i].(TextValue); !ok {
				return fmt.Errorf("validate row: column %d (%q) expects text, got %T", i, column.Name, row[i])
			}
		default:
			return fmt.Errorf("validate row: column %d (%q) has unknown type %d", i, column.Name, column.Type)
		}
	}
	return nil
}

func (s *Schema) encodeRow(row Row) []byte {
	var buf []byte
	for _, value := range row {
		buf = value.encode(buf)
	}
	return buf
}

func (s *Schema) encodeKeyFromRow(row Row) []byte {
	return row[s.PrimaryKeyIndex].encode(nil)
}

func (s *Schema) encodeKeyFromValue(v Value) []byte {
	return v.encode(nil)
}

func (s *Schema) decodeKey(buf []byte) (Value, error) {
	pkType := s.Columns[s.PrimaryKeyIndex].Type
	v, n, err := decodeValue(buf, pkType)
	if err != nil {
		return nil, err
	}
	if n != len(buf) {
		return nil, fmt.Errorf("decode key: %d trailing bytes after consuming %d", len(buf)-n, n)
	}
	return v, nil
}

// NewSchema constructs a Schema, validating that columns are non-empty,
// names are unique, and the primary key index is in range.
func NewSchema(pkIndex int, columns []Column) (*Schema, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("create schema: must have at least one column")
	}
	if pkIndex < 0 || pkIndex >= len(columns) {
		return nil, fmt.Errorf("create schema: primary key index %d out of range (%d columns)", pkIndex, len(columns))
	}

	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if column.Name == "" {
			return nil, fmt.Errorf("create schema: column has empty name")
		}
		if _, duplicate := seen[column.Name]; duplicate {
			return nil, fmt.Errorf("create schema: multiple columns with the same name %q", column.Name)
		}
		seen[column.Name] = struct{}{}
	}
	return &Schema{Columns: columns, PrimaryKeyIndex: pkIndex}, nil
}

// marshalSchema serializes the schema into its binary on-disk representation.
//
// Layout:
// [1 byte]  column count
// [1 byte]  primary key index
// per column:
// - [1 byte]  type tag
// - [1 byte]  name length (N)
// - [N bytes] name
//
// Callers must ensure column count, primary key index, and each
// name length fit in a single byte (<= 255).
func (s *Schema) marshal() []byte {
	var buf []byte
	buf = append(buf, byte(len(s.Columns)))
	buf = append(buf, byte(s.PrimaryKeyIndex))
	for i := range s.Columns {
		buf = append(buf, byte(s.Columns[i].Type))
		buf = append(buf, byte(len(s.Columns[i].Name)))
		buf = append(buf, s.Columns[i].Name...)
	}
	return buf
}

func unmarshalSchema(data []byte) (*Schema, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("unmarshal schema: header truncated: got %d bytes, need 2", len(data))
	}
	columnCount := int(data[0])
	pkIndex := int(data[1])
	offset := 2

	columns := make([]Column, columnCount)
	for i := range columnCount {
		if len(data)-offset < 2 {
			return nil, fmt.Errorf("unmarshal schema: column %d header truncated", i)
		}
		columns[i].Type = ColType(data[offset])
		nameLen := int(data[offset+1])
		offset += 2

		if len(data)-offset < nameLen {
			return nil, fmt.Errorf("unmarshal schema: column %d name truncated: need %d bytes, have %d", i, nameLen, len(data)-offset)
		}
		columns[i].Name = string(data[offset : offset+nameLen])
		offset += nameLen
	}

	if offset != len(data) {
		return nil, fmt.Errorf("unmarshal schema: %d trailing bytes after consuming %d", len(data)-offset, offset)
	}

	return NewSchema(pkIndex, columns)
}
