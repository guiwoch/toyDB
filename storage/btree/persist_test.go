package btree_test

import (
	"bytes"
	"testing"

	"github.com/guiwoch/toyDB/storage/btree"
	"github.com/guiwoch/toyDB/storage/page"
)

func TestPersistence(t *testing.T) {
	path := t.TempDir() + "/test"
	records := recordGenerator(1000)

	// Phase 1: write and close
	tree, err := btree.New(page.KeyTypeInt, path)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		tree.Insert(r.key[:], r.value[:])
		if err := tree.Close(); err != nil {
			t.Fatal(err)
		}

	}
	// Phase 2: reopen and verify
	tree, err = btree.New(page.KeyTypeInt, path)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

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
