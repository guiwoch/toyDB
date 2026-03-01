// Package btree implements a B+-tree
package btree

import (
	"encoding/binary"

	"github.com/guiwoch/toyDB/storage/page"
	"github.com/guiwoch/toyDB/storage/pager"
)

type Btree struct {
	pager *pager.Pager

	rootID      uint32
	firstLeafID uint32
	lastLeafID  uint32

	keyType uint8
}

func New(keyType uint8) *Btree {
	pgr := pager.NewPager()
	root := pgr.Allocate(page.TypeLeaf, keyType)
	return &Btree{
		pager:       pgr,
		rootID:      root.PageID(),
		firstLeafID: root.PageID(),
		lastLeafID:  root.PageID(),
		keyType:     keyType,
	}
}

// Search traverses the tree from root to leaf and returns the value associated
// with the given key. Returns (nil, false) if the key is not found.
func (b *Btree) Search(key []byte) ([]byte, bool) {
	p := b.findLeaf(key)
	return p.Get(key)
}

func (b *Btree) findLeaf(key []byte) *page.Page {
	p := b.pager.Get(b.rootID)
	for p.PageType() == page.TypeInternal {
		var childID uint32
		i, found := p.SearchKey(key)
		if found {
			// Equal keys go right, so follow the right child (i+1).
			if i == p.RecordCount()-1 {
				childID = p.RightPointer()
			} else {
				childID = binary.BigEndian.Uint32(p.ValueByIndex(i + 1))
			}
		} else {
			// Not found: i is already the correct child (insertion point).
			if i == p.RecordCount() {
				childID = p.RightPointer()
			} else {
				childID = binary.BigEndian.Uint32(p.ValueByIndex(i))
			}
		}
		b.pager.Unpin(p.PageID())
		p = b.pager.Get(childID)
	}
	return p
}
