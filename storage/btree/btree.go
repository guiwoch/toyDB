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

// Insert traverses the tree from root to leaf and inserts a value.
// It retuns an error if the key being inserted already exists.
// It manages splits using the breadcrumbs generated during traversal.
// TODO: make the splitting based on size, not indexes
func (b *Btree) Insert(key []byte, value []byte) error {
	p := b.root

	// breadcrumbs is a stack that holds the path taken by a traversal.
	// This is used for spliting and merging, as it provides
	// a way to go to parent nodes easily.
	var breadcrumbs []uint32
	for p.PageType() == page.PageTypeInternal { // builds the breadcrumbs
		i, found := p.SearchKey(key)
		breadcrumbs = append(breadcrumbs, p.PageID())

		if found { // i+1 case
			if i == p.RecordCount()-1 { // if its the last one
				p = b.pager.GetPage(p.RightPointer())
				continue
			}

			childID := binary.BigEndian.Uint32(p.ValueByIndex(i + 1))
			p = b.pager.GetPage(childID)
			continue
		}
		if i == p.RecordCount() {
			p = b.pager.GetPage(p.RightPointer())
			continue
		}
		childID := binary.BigEndian.Uint32(p.ValueByIndex(i))
		p = b.pager.GetPage(childID)
	}

	err := p.InsertRecord(key, value)
	if err == nil {
		return nil
	}

	if errors.Is(err, page.ErrPageFull) { // The page needs splitting
		for {
			mid := p.RecordCount() / 2
			midKey := make([]byte, len(p.KeyByIndex(mid)))
			copy(midKey, p.KeyByIndex(mid))

			var left, right *page.Page
			if p.PageType() == page.PageTypeLeaf {
				// Leaf split: mid stays in right half (copy up).
				left = b.pager.AllocatePageFromRecords(p.PageType(), b.keyType, p.ExtractRecords(0, mid))
				right = b.pager.AllocatePageFromRecords(p.PageType(), b.keyType, p.ExtractRecords(mid, p.RecordCount()))
			} else {
				// Internal split: mid excluded from both halves (move up).
				left = b.pager.AllocatePageFromRecords(p.PageType(), b.keyType, p.ExtractRecords(0, mid))
				right = b.pager.AllocatePageFromRecords(p.PageType(), b.keyType, p.ExtractRecords(mid+1, p.RecordCount()))
				// The mid key's left child pointer becomes the left page's RightPointer.
				left.SetRightPointer(binary.BigEndian.Uint32(p.ValueByIndex(mid)))
			}

			oldPageID := p.PageID()
			b.pager.FreePage(oldPageID)

			// No parent. Need to create a new root with the promoted key
			if len(breadcrumbs) == 0 {
				var buf [4]byte
				newRoot := b.pager.AllocatePage(page.PageTypeInternal, b.keyType)
				binary.BigEndian.PutUint32(buf[:], left.PageID())
				newRoot.InsertRecord(midKey, buf[:])
				newRoot.SetRightPointer(right.PageID())
				b.root = newRoot

				compare := bytes.Compare(key, midKey)
				if compare == -1 {
					left.InsertRecord(key, value)
				} else {
					right.InsertRecord(key, value)
				}
				return nil
			}

			parentID := breadcrumbs[len(breadcrumbs)-1]
			breadcrumbs = breadcrumbs[:len(breadcrumbs)-1]
			p = b.pager.GetPage(parentID)

			// Find the key in the parent whose child pointer referenced the old page,
			// and update it to point to the right page.
			var buf [4]byte
			for j := uint16(0); j < p.RecordCount(); j++ {
				childID := binary.BigEndian.Uint32(p.ValueByIndex(j))
				if childID == oldPageID {
					key := p.KeyByIndex(j)
					p.DeleteRecord(key)
					binary.BigEndian.PutUint32(buf[:], right.PageID())
					if err := p.InsertRecord(key, buf[:]); err != nil {
						// this Insert complements the delete as an update of the key,
						// this guarantees that the key is neither duplicate
						// nor is the page full.
						panic("error on page update")
					}
					break
				}
			}
			if p.RightPointer() == oldPageID {
				p.SetRightPointer(right.PageID())
			}

			binary.BigEndian.PutUint32(buf[:], left.PageID())
			err := p.InsertRecord(midKey, buf[:])
			if errors.Is(err, page.ErrPageFull) {
				continue
			}
			if errors.Is(err, page.ErrDuplicateKey) {
				// the promoted key should not already exist on parent pages.
				panic("promoted key is duplicate")
			}
			if err == nil {
				compare := bytes.Compare(key, midKey)
				if compare == -1 {
					left.InsertRecord(key, value)
				} else {
					right.InsertRecord(key, value)
				}
				return nil
			}

		}
	}
	return err
}
