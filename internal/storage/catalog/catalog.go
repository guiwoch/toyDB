// Package catalog stores table metadata as a B+tree keyed by table name.
package catalog

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/guiwoch/toyDB/internal/storage/btree"
)

// Row is the value stored per table in the catalog. SchemaBytes is the
// serialized schema produced by the schema package.
type Row struct {
	RootID      uint32
	SchemaBytes []byte
}

// Layout: [rootID:4][schemaLen:4][schemaBytes:N]
func encodeRow(r Row) []byte {
	buf := make([]byte, 8+len(r.SchemaBytes))
	binary.BigEndian.PutUint32(buf[0:4], r.RootID)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(r.SchemaBytes)))
	copy(buf[8:], r.SchemaBytes)
	return buf
}

func decodeRow(buf []byte) (Row, error) {
	if len(buf) < 8 {
		return Row{}, fmt.Errorf("decode catalog row: header truncated, got %d bytes, need 8", len(buf))
	}
	rootID := binary.BigEndian.Uint32(buf[0:4])
	schemaLen := binary.BigEndian.Uint32(buf[4:8])
	if len(buf)-8 < int(schemaLen) {
		return Row{}, fmt.Errorf("decode catalog row: schema truncated, need %d bytes, have %d", schemaLen, len(buf)-8)
	}
	if int(schemaLen) != len(buf)-8 {
		return Row{}, fmt.Errorf("decode catalog row: %d trailing bytes after consuming %d", len(buf)-8-int(schemaLen), 8+schemaLen)
	}
	schemaBytes := make([]byte, schemaLen)
	copy(schemaBytes, buf[8:])
	return Row{RootID: rootID, SchemaBytes: schemaBytes}, nil
}

type Catalog struct {
	tree *btree.Btree
}

func Open(tree *btree.Btree) *Catalog {
	return &Catalog{tree: tree}
}

func (c *Catalog) Lookup(name string) (Row, bool, error) {
	value, found := c.tree.Search([]byte(name))
	if !found {
		return Row{}, false, nil
	}
	row, err := decodeRow(value)
	if err != nil {
		return Row{}, false, err
	}
	return row, true, nil
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

// Delete removes the catalog entry for name. Returns btree.ErrKeyNotFound
// if the entry does not exist.
func (c *Catalog) Delete(name string) error {
	return c.tree.Delete([]byte(name))
}

// Names returns all the names of the tables on the catalog
func (c *Catalog) Names() []string {
	var names []string
	for record := range c.tree.AscendingRange(nil, nil) {
		names = append(names, string(record.Key))
	}
	return names
}
