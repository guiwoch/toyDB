package btree

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/guiwoch/toyDB/storage/page"
)

func (b *Btree) Insert(key, value []byte) error {
	type splitResult struct {
		promotedKey []byte
		left, right *page.Page
		oldPageID   uint32
	}
	var insert func(p *page.Page) (split *splitResult, err error)
	insert = func(p *page.Page) (split *splitResult, err error) {
		split = &splitResult{}

		if p.PageType() == page.TypeLeaf {
			err := p.InsertRecord(key, value)
			if err != nil {
				if errors.Is(err, page.ErrPageFull) { // leaf split
					// TODO: use space-based-splitting instead of index
					mid := p.RecordCount() / 2

					split.promotedKey = p.KeyByIndex(mid)
					split.left = b.pager.AllocatePageFromRecords(page.TypeLeaf, b.keyType, p.ExtractRecords(0, mid))
					split.right = b.pager.AllocatePageFromRecords(page.TypeLeaf, b.keyType, p.ExtractRecords(mid, p.RecordCount()))

					// update the leaf linked-list
					if p.PrevLeaf() != 0 {
						b.pager.GetPage(p.PrevLeaf()).SetNextLeaf(split.left.PageID())
					} else {
						b.firstLeafID = split.left.PageID()
					}
					split.left.SetPrevLeaf(p.PrevLeaf())
					split.left.SetNextLeaf(split.right.PageID())
					split.right.SetPrevLeaf(split.left.PageID())
					split.right.SetNextLeaf(p.NextLeaf())
					if p.NextLeaf() != 0 {
						b.pager.GetPage(p.NextLeaf()).SetPrevLeaf(split.right.PageID())
					} else {
						b.lastLeafID = split.right.PageID()
					}

					split.oldPageID = p.PageID()

					compare := bytes.Compare(key, split.promotedKey)
					if compare == 1 || compare == 0 { // key >= midKey, goes right (equal keys go right, matching Search)
						errr := split.right.InsertRecord(key, value)
						if errr != nil {
							panic("no space for insert after split")
						}
					} else {
						errr := split.left.InsertRecord(key, value)
						if errr != nil {
							panic("no space for insert after split")
						}
					}

					b.pager.FreePage(p.PageID())

					return split, err
				}
				return nil, err
			}
			return nil, nil
		}

		if p.PageType() == page.TypeInternal {
			var splitRes *splitResult
			var err error

			i, found := p.SearchKey(key)
			if found {
				// Equal keys go right, so follow the right child (i+1).
				if i == p.RecordCount()-1 {
					splitRes, err = insert(b.pager.GetPage(p.RightPointer()))
				} else {

					childID := binary.BigEndian.Uint32(p.ValueByIndex(i + 1))
					splitRes, err = insert(b.pager.GetPage(childID))
				}
			} else {
				// Not found: i is already the correct child (insertion point).
				if i == p.RecordCount() {
					splitRes, err = insert(b.pager.GetPage(p.RightPointer()))
				} else {
					childID := binary.BigEndian.Uint32(p.ValueByIndex(i))
					splitRes, err = insert(b.pager.GetPage(childID))
				}
			}

			if !errors.Is(err, page.ErrPageFull) {
				return nil, err
			}

			var buf [4]byte
			// update the old record that pointed to the page before the split
			binary.BigEndian.PutUint32(buf[:], splitRes.right.PageID())
			// the loop searches every record to find the one that points to the old page.
			for i := range p.RecordCount() {
				id := binary.BigEndian.Uint32(p.ValueByIndex(i))
				if id == splitRes.oldPageID { // found the one that needs to be updated

					// TODO:(optional) add a page.UpdateRecord(key, value)
					// Currently need to delete + reinsert to update

					updateKey := p.KeyByIndex(i)
					ok := p.DeleteRecord(updateKey)
					if !ok {
						panic("key to be updated not found")
					}

					binary.BigEndian.PutUint32(buf[:], splitRes.right.PageID())
					if err := p.InsertRecord(updateKey, buf[:]); err != nil {
						panic("no space during update")
					}
				}
			}

			// old page may have been the rightmost child
			if p.RightPointer() == splitRes.oldPageID {
				p.SetRightPointer(splitRes.right.PageID())
			}

			// insert the new record into the page
			binary.BigEndian.PutUint32(buf[:], splitRes.left.PageID())
			err = p.InsertRecord(splitRes.promotedKey, buf[:])
			if errors.Is(err, page.ErrPageFull) {
				// TODO: use space-based-splitting instead of index

				mid := p.RecordCount() / 2

				split.promotedKey = p.KeyByIndex(mid)

				split.left = b.pager.AllocatePageFromRecords(page.TypeInternal, b.keyType, p.ExtractRecords(0, mid))
				bin := binary.BigEndian.Uint32(p.ValueByIndex(mid))
				split.left.SetRightPointer(bin)
				split.right = b.pager.AllocatePageFromRecords(page.TypeInternal, b.keyType, p.ExtractRecords(mid+1, p.RecordCount()))
				split.right.SetRightPointer(p.RightPointer())

				split.oldPageID = p.PageID()

				b.pager.FreePage(p.PageID())

				compare := bytes.Compare(splitRes.promotedKey, split.promotedKey)
				if compare >= 0 { // need to insert on the right
					err := split.right.InsertRecord(splitRes.promotedKey, buf[:])
					if err != nil {
						panic("could not insert after split")
					}
				} else {
					err := split.left.InsertRecord(splitRes.promotedKey, buf[:])
					if err != nil {
						panic("could not insert after split")
					}
				}

				return split, err
			}
			return nil, err
		}
		panic("pageType is not internal nor leaf")
	}

	split, err := insert(b.pager.GetPage(b.rootID))
	if errors.Is(err, page.ErrPageFull) {

		newRoot := b.pager.AllocatePage(page.TypeInternal, b.keyType)

		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], split.left.PageID())
		newRoot.InsertRecord(split.promotedKey, buf[:])
		newRoot.SetRightPointer(split.right.PageID())

		b.rootID = newRoot.PageID()
	}
	return err
}
