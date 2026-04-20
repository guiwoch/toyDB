package btree_test

import (
	"testing"

	"github.com/guiwoch/toyDB/storage/btree"
	"github.com/guiwoch/toyDB/storage/db"
	"github.com/guiwoch/toyDB/storage/page"
)

// newTestTree opens a fresh DB backed by a temp file and returns a single
// int-keyed table. The DB is closed automatically on test cleanup.
func newTestTree(t *testing.T) *btree.Btree {
	t.Helper()
	d, err := db.Open(t.TempDir() + "/test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = d.Close()
	})
	t.Cleanup(func() {
		if n := d.PinnedCount(); n != 0 {
			t.Errorf("pin leak: %d pages still pinned after test", n)
		}
	})
	tree, err := d.CreateTable("t", page.KeyTypeInt)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}
