// Package db provides the top-level DB type: one file, one pager, one catalog
// tree, and N user-table B+trees sharing the pager.
package db

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/guiwoch/toyDB/storage/btree"
	"github.com/guiwoch/toyDB/storage/catalog"
	"github.com/guiwoch/toyDB/storage/page"
	"github.com/guiwoch/toyDB/storage/pager"
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
	freeListHead  uint32 // reserved for stage 2 (linked freelist)
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

var (
	ErrTableExists   = errors.New("table already exists")
	ErrTableNotFound = errors.New("table not found")
)

type DB struct {
	pager   *pager.Pager
	catalog *catalog.Catalog
	header  dbHeader
	open    map[string]*btree.Btree
}

// Open opens or creates a DB at the given path.
func Open(path string) (*DB, error) {
	p, fresh, err := pager.Open(path)
	if err != nil {
		return nil, err
	}
	d := &DB{
		pager: p,
		open:  make(map[string]*btree.Btree),
	}
	if fresh {
		root := p.Allocate(page.TypeLeaf, page.KeyTypeString)
		rootID := root.PageID()
		p.Unpin(rootID)
		d.catalog = catalog.Open(btree.Open(p, rootID, page.KeyTypeString))
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
	d.catalog = catalog.Open(btree.Open(p, h.catalogRootID, page.KeyTypeString))
	return d, nil
}

// CreateTable allocates a root leaf, records the table in the catalog, and
// returns the underlying btree.
func (d *DB) CreateTable(name string, keyType uint8) (*btree.Btree, error) {
	if _, ok := d.catalog.Lookup(name); ok {
		return nil, ErrTableExists
	}
	root := d.pager.Allocate(page.TypeLeaf, keyType)
	rootID := root.PageID()
	d.pager.Unpin(rootID)
	tree := btree.Open(d.pager, rootID, keyType)
	if err := d.catalog.Upsert(name, catalog.Row{RootID: rootID, KeyType: keyType}); err != nil {
		return nil, err
	}
	d.open[name] = tree
	return tree, nil
}

// OpenTable returns the btree for an existing table, caching it for the
// remainder of the DB's lifetime.
func (d *DB) OpenTable(name string) (*btree.Btree, error) {
	if tree, ok := d.open[name]; ok {
		return tree, nil
	}
	row, ok := d.catalog.Lookup(name)
	if !ok {
		return nil, ErrTableNotFound
	}
	tree := btree.Open(d.pager, row.RootID, row.KeyType)
	d.open[name] = tree
	return tree, nil
}

// Close persists the catalog and header, then closes the underlying file.
// Any tree whose root changed during the session is re-upserted first.
func (d *DB) Close() error {
	for name, tree := range d.open {
		if err := d.catalog.Upsert(name, catalog.Row{
			RootID:  tree.RootID(),
			KeyType: tree.KeyType(),
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
