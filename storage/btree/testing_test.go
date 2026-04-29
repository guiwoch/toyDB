package btree_test

import (
	"testing"

	"github.com/guiwoch/toyDB/storage/btree"
	"github.com/guiwoch/toyDB/storage/page"
	"github.com/guiwoch/toyDB/storage/pager"
)

// newTestTree opens a fresh pager backed by a temp file and returns a btree
// rooted on a freshly-allocated leaf. The pager is closed automatically on
// test cleanup, with a pin-leak check.
func newTestTree(t *testing.T) *btree.Btree {
	t.Helper()
	p, _, err := pager.Open(t.TempDir() + "/test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.Close()
	})
	t.Cleanup(func() {
		if n := p.PinnedCount(); n != 0 {
			t.Errorf("pin leak: %d pages still pinned after test", n)
		}
	})

	root := p.Allocate(page.TypeLeaf)
	rootID := root.PageID()
	p.Unpin(rootID)
	return btree.Open(p, rootID)
}
