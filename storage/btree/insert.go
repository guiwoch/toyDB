package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/guiwoch/toyDB/storage/page"
)

type splitResult struct {
	promotedKey []byte
	left, right *page.Page
	oldPageID   uint32
}

func (b *Btree) Insert(key, value []byte) error {
	root := b.pager.Get(b.rootID)
	splitRes, err := b.insert(key, value, root)
	b.pager.Unpin(root.PageID())
	if splitRes != nil {
		newRoot := b.pager.Allocate(page.TypeInternal, b.keyType)
		var leftID [4]byte
		binary.BigEndian.PutUint32(leftID[:], splitRes.left.PageID())
		newRoot.InsertRecord(splitRes.promotedKey, leftID[:])
		newRoot.SetRightPointer(splitRes.right.PageID())
		b.rootID = newRoot.PageID()
		b.pager.Unpin(newRoot.PageID())
		b.pager.Unpin(splitRes.left.PageID())
		b.pager.Unpin(splitRes.right.PageID())
	}
	return err
}

func (b *Btree) insert(key, value []byte, p *page.Page) (*splitResult, error) {
	switch p.PageType() {
	case page.TypeInternal:
		return b.insertIntoInternal(key, value, p)
	case page.TypeLeaf:
		return b.insertIntoLeaf(key, value, p)
	}
	panic("Unknown page type")
}

func (b *Btree) insertIntoInternal(key, value []byte, p *page.Page) (*splitResult, error) {
	// decent: go towards the leaf
	nextPage := b.findChild(key, p)
	splitRes, err := b.insert(key, value, nextPage)
	b.pager.Unpin(nextPage.PageID())

	// unwinding: do updating and splitting
	if splitRes != nil {
		// the function return another splitRes that might happen during the updating
		splitRes = b.updateFromSplit(splitRes, p)
	}
	return splitRes, err
}

// updateFromSplit updates the page if a split happened at one of its childs
func (b *Btree) updateFromSplit(splitRes *splitResult, p *page.Page) *splitResult {
	// find where the old page is located
	for i := range p.RecordCount() {
		if id := binary.BigEndian.Uint32(p.ValueByIndex(i)); id == splitRes.oldPageID {
			key := p.KeyByIndex(i)
			p.DeleteRecord(key)

			var rightIDBytes [4]byte
			binary.BigEndian.PutUint32(rightIDBytes[:], splitRes.right.PageID())
			err := p.InsertRecord(key, rightIDBytes[:])
			if err != nil {
				panic("no space to reinsert during record update")
			}
			break
		}
	}

	// if the page is not found during the loop, it's located on the p.RightPointer
	if p.RightPointer() == splitRes.oldPageID {
		p.SetRightPointer(splitRes.right.PageID())
	}

	// insert the promotedKey and the left page from the splitRes, split again if no space
	var leftIDBytes [4]byte
	binary.BigEndian.PutUint32(leftIDBytes[:], splitRes.left.PageID())
	err := p.InsertRecord(splitRes.promotedKey, leftIDBytes[:])
	if errors.Is(err, page.ErrPageFull) { // need to split it
		return b.splitInternal(splitRes, p)
	} else if err != nil {
		panic(fmt.Sprintf("Unexpected error during splitting: %v", err))
	}
	return nil
}

func (b *Btree) splitInternal(pendingSplit *splitResult, p *page.Page) *splitResult {
	split := &splitResult{}
	split.oldPageID = p.PageID()

	midIdx := p.RecordCount() / 2 // TODO: Update to space-based splitting
	midKey := p.KeyByIndex(midIdx)

	// midPoint is promoted to the parent, so it's excluded from both children
	leftRecords := p.ExtractRecords(0, midIdx)
	rightRecords := p.ExtractRecords(midIdx+1, p.RecordCount())

	split.left = b.pager.AllocateFromRecords(page.TypeInternal, b.keyType, leftRecords)
	split.right = b.pager.AllocateFromRecords(page.TypeInternal, b.keyType, rightRecords)
	split.promotedKey = midKey

	midKeyBytes := binary.BigEndian.Uint32(p.ValueByIndex(midIdx))
	split.left.SetRightPointer(midKeyBytes)
	split.right.SetRightPointer(p.RightPointer())

	// insert the pending promoted key into the correct half
	var leftID [4]byte
	binary.BigEndian.PutUint32(leftID[:], pendingSplit.left.PageID())
	var err error
	if bytes.Compare(pendingSplit.promotedKey, midKey) >= 0 {
		err = split.right.InsertRecord(pendingSplit.promotedKey, leftID[:])
	} else {
		err = split.left.InsertRecord(pendingSplit.promotedKey, leftID[:])
	}
	if err != nil {
		panic("no space for insert after internal split")
	}

	b.pager.Free(p.PageID())
	return split
}

// findChild returns returns the page that needs to be followed to insert the record, given a key and a page.
func (b *Btree) findChild(key []byte, p *page.Page) (nextPage *page.Page) {
	i, found := p.SearchKey(key)
	var idx uint16
	if found { // if found, the index needs to be i+1
		idx = i + 1
	} else {
		idx = i
	}

	var childID uint32
	if idx == p.RecordCount() {
		childID = p.RightPointer()
	} else {
		childID = binary.BigEndian.Uint32(p.ValueByIndex(idx))
	}

	return b.pager.Get(childID)
}

func (b *Btree) insertIntoLeaf(key, value []byte, p *page.Page) (*splitResult, error) {
	err := p.InsertRecord(key, value)
	if !errors.Is(err, page.ErrPageFull) {
		return nil, err
	}

	splitRes := b.splitLeaf(key, value, p)
	return splitRes, nil
}

func (b *Btree) splitLeaf(key, value []byte, p *page.Page) *splitResult {
	split := &splitResult{}
	midIdx := p.RecordCount() / 2 // TODO: Change to space-based splitting
	midKey := p.KeyByIndex(midIdx)

	leftRecords := p.ExtractRecords(0, midIdx)
	rightRecords := p.ExtractRecords(midIdx, p.RecordCount())

	split.left = b.pager.AllocateFromRecords(page.TypeLeaf, b.keyType, leftRecords)
	split.right = b.pager.AllocateFromRecords(page.TypeLeaf, b.keyType, rightRecords)
	split.promotedKey = midKey
	split.oldPageID = p.PageID()

	// update leaf linked-list
	split.left.SetPrevLeaf(p.PrevLeaf())
	split.left.SetNextLeaf(split.right.PageID())
	split.right.SetPrevLeaf(split.left.PageID())
	split.right.SetNextLeaf(p.NextLeaf())

	if p.PrevLeaf() != 0 {
		prev := b.pager.Get(p.PrevLeaf())
		prev.SetNextLeaf(split.left.PageID())
		b.pager.Unpin(prev.PageID())
	} else {
		b.firstLeafID = split.left.PageID()
	}
	if p.NextLeaf() != 0 {
		next := b.pager.Get(p.NextLeaf())
		next.SetPrevLeaf(split.right.PageID())
		b.pager.Unpin(next.PageID())
	} else {
		b.lastLeafID = split.right.PageID()
	}

	// insert the key into the correct half
	var err error
	if bytes.Compare(key, midKey) >= 0 {
		err = split.right.InsertRecord(key, value)
	} else {
		err = split.left.InsertRecord(key, value)
	}
	if err != nil {
		panic("no space for insert after leaf split")
	}

	b.pager.Free(p.PageID())
	return split
}
