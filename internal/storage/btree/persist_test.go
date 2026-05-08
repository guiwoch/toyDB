package btree_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/guiwoch/toyDB/internal/storage/btree"
	"github.com/guiwoch/toyDB/internal/storage/page"
	"github.com/guiwoch/toyDB/internal/storage/pager"
)

func TestPersistence(t *testing.T) {
	path := t.TempDir() + "/test"
	records := recordGenerator(1000)

	// Phase 1: open fresh pager, allocate a root, insert, persist rootID in page 0.
	p, _, err := pager.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	root, err := p.Allocate(page.TypeLeaf)
	if err != nil {
		t.Fatal(err)
	}
	rootID := root.PageID()
	p.Unpin(rootID)
	tree, err := btree.Open(p, rootID)
	if err != nil {
		t.Fatal(err)
	}
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
	tree, err = btree.Open(p, rootID)
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range records {
		value, err := tree.Search(r.key[:])
		if errors.Is(err, btree.ErrKeyNotFound) {
			t.Errorf("key %v not found after reopen", r.key)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value, r.value[:]) {
			t.Errorf("wrong value for key %v: got %v, want %v", r.key, value, r.value)
		}
	}
}
