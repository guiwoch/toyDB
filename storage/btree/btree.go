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
		i, found := p.SearchKey(key)
		var idx uint16
		if found {
			idx = i + 1
		} else {
			idx = i
		}
		childID := b.findChildID(p, idx)
		b.pager.Unpin(p.PageID())
		p = b.pager.Get(childID)
	}
	return p
}

// findChildID returns the page ID of the child at the slot index
// If the index equals the record count, the RightPointer is returned.
func (b *Btree) findChildID(parent *page.Page, idx uint16) uint32 {
	if idx == parent.RecordCount() {
		return parent.RightPointer()
	}
	return binary.BigEndian.Uint32(parent.ValueByIndex(idx))
}

// findChild returns the child page of p that the given key belongs to.
func (b *Btree) findChild(key []byte, p *page.Page) *page.Page {
	i, found := p.SearchKey(key)
	var idx uint16
	if found { // equal keys go right, so follow the child at i+1
		idx = i + 1
	} else {
		idx = i
	}
	return b.pager.Get(b.findChildID(p, idx))
}
