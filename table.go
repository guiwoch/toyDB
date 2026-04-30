package toydb

import (
	"iter"

	"github.com/guiwoch/toyDB/internal/storage/btree"
)

// Table is a handle to one user table inside a [DB]. Obtain a Table with
// [DB.CreateTable] or [DB.OpenTable]. A Table is valid until the parent
// DB is closed.
type Table struct {
	name   string
	schema *Schema
	tree   *btree.Btree
}

// Name returns the table's name as it was passed to CreateTable.
func (t *Table) Name() string { return t.name }

// Schema returns the table's schema. The returned pointer is shared with
// the table; use [Schema.Columns] and [Schema.PrimaryKey] for read-only
// inspection.
func (t *Table) Schema() *Schema { return t.schema }

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

// Scan returns rows with primary keys in [lo, hi), ascending. The upper
// bound is exclusive. A nil bound is unbounded on that side; Scan(nil, nil)
// returns every row. Returns ErrKeyTypeMismatch if either bound's type
// does not match the primary key column type.
func (t *Table) Scan(lo, hi Value) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		var loKey, hiKey []byte
		if lo != nil {
			if err := t.schema.validateKey(lo); err != nil {
				yield(nil, err)
				return
			}
			loKey = t.schema.encodeKeyFromValue(lo)
		}
		if hi != nil {
			if err := t.schema.validateKey(hi); err != nil {
				yield(nil, err)
				return
			}
			hiKey = t.schema.encodeKeyFromValue(hi)
		}
		recs := t.tree.AscendingRange(loKey, hiKey)
		for r := range recs {
			pk, err := t.schema.decodeKey(r.Key)
			if err != nil {
				yield(nil, err)
				return
			}
			row, err := t.schema.decodeRow(pk, r.Value)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(row, nil) {
				return
			}
		}
	}
}

// ScanDescending returns rows with primary keys in (lo, hi], descending.
// The upper bound is inclusive and the lower bound is exclusive (the
// mirror of [Table.Scan]). A nil bound is unbounded on that side;
// ScanDescending(nil, nil) returns every row. Returns ErrKeyTypeMismatch
// if either bound's type does not match the primary key column type.
func (t *Table) ScanDescending(lo, hi Value) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		var loKey, hiKey []byte
		if lo != nil {
			if err := t.schema.validateKey(lo); err != nil {
				yield(nil, err)
				return
			}
			loKey = t.schema.encodeKeyFromValue(lo)
		}
		if hi != nil {
			if err := t.schema.validateKey(hi); err != nil {
				yield(nil, err)
				return
			}
			hiKey = t.schema.encodeKeyFromValue(hi)
		}

		recs := t.tree.DescendingRange(loKey, hiKey)
		for r := range recs {
			pk, err := t.schema.decodeKey(r.Key)
			if err != nil {
				yield(nil, err)
				return
			}
			row, err := t.schema.decodeRow(pk, r.Value)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(row, nil) {
				return
			}
		}
	}
}
