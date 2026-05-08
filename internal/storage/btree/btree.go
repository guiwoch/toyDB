// Package btree implements a B+-tree
package btree

import (
	"encoding/binary"

	"github.com/guiwoch/toyDB/internal/storage/page"
	"github.com/guiwoch/toyDB/internal/storage/pager"
)

type Btree struct {
	pager *pager.Pager

	rootID      uint32
	firstLeafID uint32
	lastLeafID  uint32
}

// Open returns a Btree rooted at the given page. It descends the tree once to
// cache the leftmost and rightmost leaf IDs; these are maintained by insert
// and delete afterwards.
func Open(p *pager.Pager, rootID uint32) (*Btree, error) {
	b := &Btree{pager: p, rootID: rootID}
	first, err := b.findLeftmostLeaf()
	if err != nil {
		return nil, err
	}
	b.firstLeafID = first
	last, err := b.findRightmostLeaf()
	if err != nil {
		return nil, err
	}
	b.lastLeafID = last
	return b, nil
}

// Destroy frees every page in the tree via a BFS from the root. The Btree
// is unusable after this call.
func (b *Btree) Destroy() error {
	var frontier []uint32
	frontier = append(frontier, b.rootID)
	for len(frontier) > 0 {
		// pop
		head := frontier[0]
		frontier = frontier[1:]

		p, err := b.pager.Get(head)
		if err != nil {
			return err
		}
		if p.PageType() == page.TypeInternal {
			for i := range p.RecordCount() {
				frontier = append(frontier, binary.BigEndian.Uint32(p.ValueByIndex(i)))
			}
			frontier = append(frontier, p.RightPointer())
		}
		b.pager.Free(p.PageID())
	}
	return nil
}

func (b *Btree) RootID() uint32 { return b.rootID }

func (b *Btree) findLeftmostLeaf() (uint32, error) {
	p, err := b.pager.Get(b.rootID)
	if err != nil {
		return 0, err
	}
	for p.PageType() == page.TypeInternal {
		var childID uint32
		if p.RecordCount() > 0 {
			childID = binary.BigEndian.Uint32(p.ValueByIndex(0))
		} else {
			childID = p.RightPointer()
		}
		b.pager.Unpin(p.PageID())
		p, err = b.pager.Get(childID)
		if err != nil {
			return 0, err
		}
	}
	id := p.PageID()
	b.pager.Unpin(id)
	return id, nil
}

func (b *Btree) findRightmostLeaf() (uint32, error) {
	p, err := b.pager.Get(b.rootID)
	if err != nil {
		return 0, err
	}
	for p.PageType() == page.TypeInternal {
		childID := p.RightPointer()
		b.pager.Unpin(p.PageID())
		p, err = b.pager.Get(childID)
		if err != nil {
			return 0, err
		}
	}
	id := p.PageID()
	b.pager.Unpin(id)
	return id, nil
}

// Search traverses the tree from root to leaf and returns the value associated
// with the given key. Returns ErrKeyNotFound if the key is not present.
func (b *Btree) Search(key []byte) ([]byte, error) {
	p, err := b.findLeaf(key)
	if err != nil {
		return nil, err
	}
	value, found := p.Get(key)
	b.pager.Unpin(p.PageID())
	if !found {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

func (b *Btree) findLeaf(key []byte) (*page.Page, error) {
	p, err := b.pager.Get(b.rootID)
	if err != nil {
		return nil, err
	}
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
		p, err = b.pager.Get(childID)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

// findChildID returns the page ID of the child at slot idx in parent.
// If idx equals the record count, the RightPointer is returned.
// If idx is out of bounds, 0 (the null page) is returned.
func (b *Btree) findChildID(parent *page.Page, idx uint16) uint32 {
	if idx > parent.RecordCount() {
		return 0
	}
	if idx == parent.RecordCount() {
		return parent.RightPointer()
	}
	return binary.BigEndian.Uint32(parent.ValueByIndex(idx))
}

// findChild returns the child page of p that the given key belongs to.
func (b *Btree) findChild(key []byte, p *page.Page) (*page.Page, error) {
	i, found := p.SearchKey(key)
	var idx uint16
	if found { // equal keys go right, so follow the child at i+1
		idx = i + 1
	} else {
		idx = i
	}
	return b.pager.Get(b.findChildID(p, idx))
}
