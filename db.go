// Package toydb is an embedded key-value store with typed rows and primary-key
// indexing. A single file holds one or more tables; each table is indexed
// by its primary key.
//
// Users define a [Schema] (columns plus a primary key), create a [Table]
// via [DB.CreateTable], and operate on rows through [Table.Insert],
// [Table.Get], [Table.Update], [Table.Delete], and the [Table.Scan] /
// [Table.ScanDescending] iterators.
//
// Not yet supported: concurrent access, transactions, crash
// recovery, SQL. The DB is single-process, single-threaded, and durable
// only at [DB.Close]. See [Value] for the closed set of supported column
// types.
package toydb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/guiwoch/toyDB/internal/storage/btree"
	"github.com/guiwoch/toyDB/internal/storage/catalog"
	"github.com/guiwoch/toyDB/internal/storage/page"
	"github.com/guiwoch/toyDB/internal/storage/pager"
)

const (
	magicNumber    = 0x54444231 // "TDB1"
	currentVersion = 1
	headerSize     = 20
)

type dbHeader struct {
	magic         uint32
	version       uint32
	catalogRootID uint32
	freeListHead  uint32
}

func (h dbHeader) encode() []byte {
	buf := make([]byte, headerSize)
	binary.BigEndian.PutUint32(buf[0:4], h.magic)
	binary.BigEndian.PutUint32(buf[4:8], h.version)
	binary.BigEndian.PutUint32(buf[8:12], h.catalogRootID)
	binary.BigEndian.PutUint32(buf[12:16], h.freeListHead)
	return buf
}

func decodeHeader(buf []byte) (dbHeader, error) {
	if len(buf) < headerSize {
		return dbHeader{}, errors.New("db header truncated")
	}
	h := dbHeader{
		magic:         binary.BigEndian.Uint32(buf[0:4]),
		version:       binary.BigEndian.Uint32(buf[4:8]),
		catalogRootID: binary.BigEndian.Uint32(buf[8:12]),
		freeListHead:  binary.BigEndian.Uint32(buf[12:16]),
	}
	if h.magic != magicNumber {
		return dbHeader{}, fmt.Errorf("bad db magic: 0x%08x", h.magic)
	}
	if h.version != currentVersion {
		return dbHeader{}, fmt.Errorf("unsupported db version: %d", h.version)
	}
	return h, nil
}

// Sentinel errors returned by DB and Table methods. Callers can match them
// with errors.Is. Wrapped errors carry a human-readable detail in their
// message but preserve the sentinel for matching.
var (
	// ErrTableExists is returned by CreateTable when a table with the same
	// name already exists.
	ErrTableExists = errors.New("table already exists")

	// ErrTableNotFound is returned by OpenTable and DropTable when no
	// table with the given name exists.
	ErrTableNotFound = errors.New("table not found")

	// ErrNotFound is returned by Get when no row matches the given key.
	ErrNotFound = errors.New("row not found")

	// ErrSchemaMismatch is returned by Insert and Update when a row's
	// length or column types do not match the table schema.
	ErrSchemaMismatch = errors.New("row does not match schema")

	// ErrKeyTypeMismatch is returned by Get, Delete, Scan, and
	// ScanDescending when a key argument's runtime type does not match
	// the primary key column type.
	ErrKeyTypeMismatch = errors.New("key value type does not match primary key column type")
)

// Option configures optional DB behavior.
type Option func(*options)

type options struct {
	pagerOpts []pager.Option
}

// WithCacheSize sets the maximum number of pages held in the buffer pool.
// Defaults to [pager.DefaultCacheSize] (1024 pages, ~8 MiB). A value of 0
// disables the cap.
func WithCacheSize(n int) Option {
	return func(o *options) {
		o.pagerOpts = append(o.pagerOpts, pager.WithCacheSize(n))
	}
}

// DB is a handle to an open database file.
type DB struct {
	pager   *pager.Pager
	catalog *catalog.Catalog
	header  dbHeader
	open    map[string]*Table
}

// Open opens the DB at path, creating a new file if none exists. The
// returned DB must be closed with Close to persist any changes; durability
// is guaranteed by Close, not by individual writes.
func Open(path string, opts ...Option) (*DB, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	p, fresh, err := pager.Open(path, o.pagerOpts...)
	if err != nil {
		return nil, err
	}
	d := &DB{
		pager: p,
		open:  make(map[string]*Table),
	}
	if fresh {
		root := p.Allocate(page.TypeLeaf)
		rootID := root.PageID()
		p.Unpin(rootID)
		d.catalog = catalog.Open(btree.Open(p, rootID))
		d.header = dbHeader{
			magic:         magicNumber,
			version:       currentVersion,
			catalogRootID: rootID,
		}
		// Write an initial durable state so a crash before Close still leaves
		// the file with a valid header and catalog root.
		if err := p.Flush(); err != nil {
			p.Close()
			return nil, err
		}
		if err := p.WritePage0(d.header.encode()); err != nil {
			p.Close()
			return nil, err
		}
		return d, nil
	}

	buf := make([]byte, headerSize)
	if err := p.ReadPage0(buf); err != nil {
		p.Close()
		return nil, err
	}
	h, err := decodeHeader(buf)
	if err != nil {
		p.Close()
		return nil, err
	}
	d.header = h
	p.SetFreeListHead(h.freeListHead)
	d.catalog = catalog.Open(btree.Open(p, h.catalogRootID))
	return d, nil
}

// CreateTable creates a new table with the given schema.
// Returns [ErrTableExists] if a table with that name already exists.
func (d *DB) CreateTable(name string, s *Schema) (*Table, error) {
	if _, ok, err := d.catalog.Lookup(name); err != nil {
		return nil, err
	} else if ok {
		return nil, ErrTableExists
	}
	root := d.pager.Allocate(page.TypeLeaf)
	rootID := root.PageID()
	d.pager.Unpin(rootID)
	tree := btree.Open(d.pager, rootID)
	if err := d.catalog.Upsert(name, catalog.Row{
		RootID:      rootID,
		SchemaBytes: s.marshal(),
	}); err != nil {
		return nil, err
	}
	t := &Table{name: name, schema: s, tree: tree}
	d.open[name] = t
	return t, nil
}

// DropTable removes a table and frees its pages. Returns ErrTableNotFound
// if no table with the given name exists.
func (d *DB) DropTable(name string) error {
	table, err := d.OpenTable(name)
	if err != nil {
		return err
	}
	if err := d.catalog.Delete(name); err != nil {
		return err
	}
	delete(d.open, name)
	table.tree.Destroy()
	return nil
}

// OpenTable returns the Table for an existing table name, caching it for the
// remainder of the DB's lifetime.
func (d *DB) OpenTable(name string) (*Table, error) {
	if t, ok := d.open[name]; ok {
		return t, nil
	}
	row, ok, err := d.catalog.Lookup(name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrTableNotFound
	}
	s, err := unmarshalSchema(row.SchemaBytes)
	if err != nil {
		return nil, err
	}
	tree := btree.Open(d.pager, row.RootID)
	t := &Table{name: name, schema: s, tree: tree}
	d.open[name] = t
	return t, nil
}

// Close persists the catalog and header, then closes the underlying file.
// Any table whose root changed during the session is re-upserted first.
func (d *DB) Close() error {
	for name, t := range d.open {
		if err := d.catalog.Upsert(name, catalog.Row{
			RootID:      t.tree.RootID(),
			SchemaBytes: t.schema.marshal(),
		}); err != nil {
			return err
		}
	}
	d.header.catalogRootID = d.catalog.RootID()
	d.header.freeListHead = d.pager.FreeListHead()

	if err := d.pager.Flush(); err != nil {
		return err
	}
	if err := d.pager.WritePage0(d.header.encode()); err != nil {
		return err
	}
	return d.pager.Close()
}

// Tables returns the names of all tables in the DB, in unspecified order.
func (d *DB) Tables() []string {
	return d.catalog.Names()
}
