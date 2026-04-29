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

// Insert encodes and stores a row. Returns ErrSchemaMismatch if the row's
// shape or types do not match the table schema.
func (t *Table) Insert(row Row) error {
	if err := t.schema.validateRow(row); err != nil {
		return err
	}
	key := t.schema.encodeKeyFromRow(row)
	val := t.schema.encodeRow(row)
	return t.tree.Insert(key, val)
}

// Get returns the row with the given primary key value. Returns ErrNotFound
// if no row exists with that key, or ErrKeyTypeMismatch if keyVal's type
// does not match the primary key column type.
func (t *Table) Get(keyVal Value) (Row, error) {
	if err := t.schema.validateKey(keyVal); err != nil {
		return nil, err
	}
	key := t.schema.encodeKeyFromValue(keyVal)
	val, ok := t.tree.Search(key)
	if !ok {
		return nil, ErrNotFound
	}
	return t.schema.decodeRow(keyVal, val)
}

// Update replaces the row with the matching primary key. Returns
// ErrSchemaMismatch if the row's shape or types do not match the schema.
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

// Delete removes the row with the given primary key value. Returns
// ErrKeyTypeMismatch if keyVal's type does not match the primary key
// column type.
func (t *Table) Delete(keyVal Value) error {
	if err := t.schema.validateKey(keyVal); err != nil {
		return err
	}
	key := t.schema.encodeKeyFromValue(keyVal)
	return t.tree.Delete(key)
}

// Scan returns rows with primary keys in [lo, hi], ascending. Returns
// ErrKeyTypeMismatch if either bound's type does not match the primary
// key column type.
func (t *Table) Scan(lo, hi Value) ([]Row, error) {
	if err := t.schema.validateKey(lo); err != nil {
		return nil, err
	}
	if err := t.schema.validateKey(hi); err != nil {
		return nil, err
	}
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

// ScanDescending returns rows with primary keys in [lo, hi], descending.
// Returns ErrKeyTypeMismatch if either bound's type does not match the
// primary key column type.
func (t *Table) ScanDescending(hi, lo Value) ([]Row, error) {
	if err := t.schema.validateKey(hi); err != nil {
		return nil, err
	}
	if err := t.schema.validateKey(lo); err != nil {
		return nil, err
	}
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
