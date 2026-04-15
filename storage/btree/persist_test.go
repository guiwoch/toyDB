package btree_test

import (
	"bytes"
	"testing"

	"github.com/guiwoch/toyDB/storage/db"
	"github.com/guiwoch/toyDB/storage/page"
)

func TestPersistence(t *testing.T) {
	path := t.TempDir() + "/test"
	records := recordGenerator(1000)

	// Phase 1: write and close
	d, err := db.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := d.CreateTable("t", page.KeyTypeInt)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		tree.Insert(r.key[:], r.value[:])
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: reopen and verify
	d, err = db.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	tree, err = d.OpenTable("t")
	if err != nil {
		t.Fatal(err)
	}

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
