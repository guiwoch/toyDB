// Package btree implements a B+-tree
package btree

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/guiwoch/toyDB/storage/page"
	"github.com/guiwoch/toyDB/storage/pager"
)

type Btree struct {
	pager   *pager.Pager
	root    *page.Page
	keyType uint8
}

func New(keyType uint8) *Btree {
	pgr := pager.NewPager()
	return &Btree{
		pager:   pgr,
		root:    pgr.AllocatePage(page.PageTypeLeaf, keyType),
		keyType: keyType,
	}
}

// Search traverses the tree from root to leaf and returns the value associated
// with the given key. Returns (nil, false) if the key is not found.
func (b *Btree) Search(key []byte) ([]byte, bool) {
	p := b.root
	for p.PageType() == page.PageTypeInternal {
		i, found := p.SearchKey(key)
		if found {
			// Equal keys go right, so follow the right child (i+1).
			if i == p.RecordCount()-1 {
				p = b.pager.GetPage(p.RightPointer())
				continue
			}
			childID := binary.BigEndian.Uint32(p.ValueByIndex(i + 1))
			p = b.pager.GetPage(childID)
			continue
		}
		// Not found: i is already the correct child (insertion point).
		if i == p.RecordCount() {
			p = b.pager.GetPage(p.RightPointer())
			continue
		}
		childID := binary.BigEndian.Uint32(p.ValueByIndex(i))
		p = b.pager.GetPage(childID)
	}
	return p.Get(key)
}

	}

}

