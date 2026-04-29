package db

import (
	"github.com/guiwoch/toyDB/storage/btree"
)

type Table struct {
	name   string
	schema *Schema
	tree   *btree.Btree
}

func (t *Table) Name() string { return t.name }

func (t *Table) Insert(row Row) error {
	if err := t.schema.validateRow(row); err != nil {
		return err
	}
	key := t.schema.encodeKeyFromRow(row)
	val := t.schema.encodeRow(row)
	return t.tree.Insert(key, val)
}

func (t *Table) Get(keyVal Value) (Row, bool, error) {
	key := t.schema.encodeKeyFromValue(keyVal)
	val, ok := t.tree.Search(key)
	if !ok {
		return nil, false, nil
	}
	row, err := t.schema.decodeRow(keyVal, val)
	if err != nil {
		return nil, false, err
	}
	return row, true, nil
}

func (t *Table) Update(row Row) error {
	if err := t.schema.validateRow(row); err != nil {
		return err
	}
	key := t.schema.encodeKeyFromRow(row)
	val := t.schema.encodeRow(row)
	if err := t.tree.Delete(key); err != nil {
		return err
	}
	if err := t.tree.Insert(key, val); err != nil {
		return err
	}

	return nil
}

func (t *Table) Delete(keyVal Value) error {
	key := t.schema.encodeKeyFromValue(keyVal)
	return t.tree.Delete(key)
}

func (t *Table) Scan(lo, hi Value) ([]Row, error) {
	from := t.schema.encodeKeyFromValue(lo)
	to := t.schema.encodeKeyFromValue(hi)
	recs := t.tree.AscendingRange(from, to)
	rows := make([]Row, 0, len(recs))
	for _, r := range recs {
		pk, err := t.schema.decodeKey(r.Key)
		if err != nil {
			return nil, err
		}
		row, err := t.schema.decodeRow(pk, r.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (t *Table) ScanDescending(hi, lo Value) ([]Row, error) {
	from := t.schema.encodeKeyFromValue(hi)
	to := t.schema.encodeKeyFromValue(lo)

	recs := t.tree.DescendingRange(from, to)
	rows := make([]Row, 0, len(recs))
	for _, r := range recs {
		pk, err := t.schema.decodeKey(r.Key)
		if err != nil {
			return nil, err
		}
		row, err := t.schema.decodeRow(pk, r.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}
