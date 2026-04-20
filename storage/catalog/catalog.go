// Package catalog stores table metadata as a B+tree keyed by table name.
package catalog

import (
	"encoding/binary"
	"errors"

	"github.com/guiwoch/toyDB/storage/btree"
)

const rowSize = 5

// Row is the value stored per table in the catalog.
type Row struct {
	RootID  uint32
	KeyType uint8
}

func encodeRow(r Row) []byte {
	buf := make([]byte, rowSize)
	binary.BigEndian.PutUint32(buf[0:4], r.RootID)
	buf[4] = r.KeyType
	return buf
}

func decodeRow(buf []byte) Row {
	return Row{
		RootID:  binary.BigEndian.Uint32(buf[0:4]),
		KeyType: buf[4],
	}
}

type Catalog struct {
	tree *btree.Btree
}

func Open(tree *btree.Btree) *Catalog {
	return &Catalog{tree: tree}
}

func (c *Catalog) Lookup(name string) (Row, bool) {
	value, found := c.tree.Search([]byte(name))
	if !found {
		return Row{}, false
	}
	return decodeRow(value), true
}

// Upsert writes the row for the given table name. If a row already exists, it
// is replaced. (Update + Insert)
func (c *Catalog) Upsert(name string, row Row) error {
	key := []byte(name)
	if err := c.tree.Delete(key); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
		return err
	}
	return c.tree.Insert(key, encodeRow(row))
}

// RootID returns the current root page ID of the catalog tree.
func (c *Catalog) RootID() uint32 {
	return c.tree.RootID()
}
