package btree_test

import (
	"errors"
	"testing"

	"github.com/guiwoch/toyDB/storage/btree"
	"github.com/guiwoch/toyDB/storage/page"
)

func uniqueRecords(n uint32) []record {
	seen := make(map[[4]byte]struct{})
	var unique []record
	for _, r := range recordGenerator(n) {
		if _, ok := seen[r.key]; ok {
			continue
		}
		seen[r.key] = struct{}{}
		unique = append(unique, r)
	}
	return unique
}

func TestDeleteNotFound(t *testing.T) {
	tree := btree.New(page.KeyTypeInt)
	records := uniqueRecords(100)
	for _, r := range records {
		tree.Insert(r.key[:], r.value[:])
	}

	var missing [4]byte
	err := tree.Delete(missing[:])
	if !errors.Is(err, btree.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDeleteAndSearch(t *testing.T) {
	tree := btree.New(page.KeyTypeInt)
	records := uniqueRecords(10_000_000)
	for _, r := range records {
		tree.Insert(r.key[:], r.value[:])
	}

	for _, r := range records {
		if err := tree.Delete(r.key[:]); err != nil {
			t.Errorf("unexpected error deleting key %v: %v", r.key, err)
		}

		_, found := tree.Search(r.key[:])
		if found {
			t.Errorf("expected key %v to be absent after deletion", r.key)
		}
	}
}

func TestDeleteAllAndReinsert(t *testing.T) {
	tree := btree.New(page.KeyTypeInt)
	records := uniqueRecords(10_000_000)
	for _, r := range records {
		tree.Insert(r.key[:], r.value[:])
	}
	for _, r := range records {
		tree.Delete(r.key[:])
	}

	for _, r := range records {
		tree.Insert(r.key[:], r.value[:])
	}
	for _, r := range records {
		_, found := tree.Search(r.key[:])
		if !found {
			t.Errorf("expected key %v to be present after reinsert", r.key)
		}
	}
}
