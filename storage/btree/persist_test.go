package btree_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/guiwoch/toyDB/storage/btree"
	"github.com/guiwoch/toyDB/storage/page"
	"github.com/guiwoch/toyDB/storage/pager"
)

func TestPersistence(t *testing.T) {
	path := t.TempDir() + "/test"
	records := recordGenerator(1000)

	// Phase 1: open fresh pager, allocate a root, insert, persist rootID in page 0.
	p, _, err := pager.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	root := p.Allocate(page.TypeLeaf)
	rootID := root.PageID()
	p.Unpin(rootID)
	tree := btree.Open(p, rootID)
	for _, r := range records {
		tree.Insert(r.key[:], r.value[:])
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], rootID)
	if err := p.WritePage0(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: reopen, recover rootID from page 0, verify all records.
	p, _, err = pager.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.ReadPage0(hdr[:]); err != nil {
		t.Fatal(err)
	}
	rootID = binary.BigEndian.Uint32(hdr[:])
	tree = btree.Open(p, rootID)

	for _, r := range records {
		value, found := tree.Search(r.key[:])
		if !found {
			t.Errorf("key %v not found after reopen", r.key)
			continue
		}
		if !bytes.Equal(value, r.value[:]) {
			t.Errorf("wrong value for key %v: got %v, want %v", r.key, value, r.value)
		}
	}
}
