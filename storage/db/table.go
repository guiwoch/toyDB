package db

import (
	"github.com/guiwoch/toyDB/storage/btree"
	"github.com/guiwoch/toyDB/storage/schema"
)

type Table struct {
	name   string
	schema *schema.Schema
	tree   *btree.Btree
}

func (t *Table) Name() string           { return t.name }
func (t *Table) Schema() *schema.Schema { return t.schema }
func (t *Table) Tree() *btree.Btree     { return t.tree }

func (t *Table) Insert(row schema.Row) error {
	if err := t.schema.ValidateRow(row); err != nil {
		return err
	}
	key := t.schema.EncodeKeyFromRow(row)
	val := t.schema.EncodeRow(row)
	return t.tree.Insert(key, val)
}

func (t *Table) Get(keyVal schema.Value) (schema.Row, bool, error) {
	key := t.schema.EncodeKeyFromValue(keyVal)
	val, ok := t.tree.Search(key)
	if !ok {
		return nil, false, nil
	}
	row, err := t.schema.DecodeRow(val)
	if err != nil {
		return nil, false, err
	}
	return row, true, nil
}

func (t *Table) Update(row schema.Row) error {
	if err := t.schema.ValidateRow(row); err != nil {
		return err
	}
	key := t.schema.EncodeKeyFromRow(row)
	val := t.schema.EncodeRow(row)
	if err := t.tree.Delete(key); err != nil {
		return err
	}
	if err := t.tree.Insert(key, val); err != nil {
		return err
	}

	return nil
}

func (t *Table) Delete(keyVal schema.Value) error {
	key := t.schema.EncodeKeyFromValue(keyVal)
	return t.tree.Delete(key)
}

func (t *Table) Scan(lo, hi schema.Value) ([]schema.Row, error) {
	from := t.schema.EncodeKeyFromValue(lo)
	to := t.schema.EncodeKeyFromValue(hi)
	recs := t.tree.AscendingRange(from, to)
	rows := make([]schema.Row, 0, len(recs))
	for _, r := range recs {
		row, err := t.schema.DecodeRow(r.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (t *Table) ScanDescending(hi, lo schema.Value) ([]schema.Row, error) {
	from := t.schema.EncodeKeyFromValue(hi)
	to := t.schema.EncodeKeyFromValue(lo)

	recs := t.tree.DescendingRange(from, to)
	rows := make([]schema.Row, 0, len(recs))
	for _, r := range recs {
		row, err := t.schema.DecodeRow(r.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}
