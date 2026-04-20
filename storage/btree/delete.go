package btree

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/guiwoch/toyDB/storage/page"
)

var ErrKeyNotFound = errors.New("key not found")

func (b *Btree) Delete(key []byte) error {
	root := b.pager.Get(b.rootID)
	underflow, err := b.delete(key, root)
	b.pager.Unpin(root.PageID())
	if underflow {
		b.collapseRoot()
	}
	return err
}

func (b *Btree) collapseRoot() {
	root := b.pager.Get(b.rootID)
	// the leaf root cannot be collapsed
	if root.PageType() == page.TypeLeaf || root.RecordCount() > 0 {
		b.pager.Unpin(root.PageID())
		return
	}
	b.rootID = root.RightPointer()
	b.pager.Free(root.PageID())
}

func (b *Btree) delete(key []byte, p *page.Page) (bool, error) {
	switch p.PageType() {
	case page.TypeInternal:
		return b.deleteOnInternal(key, p)
	case page.TypeLeaf:
		return b.deleteOnLeaf(key, p)
	}
	panic(fmt.Sprintf("unknown page type: %v", p.PageType()))
}

func (b *Btree) deleteOnInternal(key []byte, p *page.Page) (bool, error) {
	i, found := p.SearchKey(key)
	var childIdx uint16
	if found {
		childIdx = i + 1
	} else {
		childIdx = i
	}

	childPage := b.findChild(key, p)
	underflow, err := b.delete(key, childPage)
	if err != nil {
		b.pager.Unpin(childPage.PageID())
		return false, err
	}

	// unwinding:
	if !underflow {
		b.pager.Unpin(childPage.PageID())
		return false, nil
	}

	b.steal(p, childIdx)
	if childPage.BytesUntilUnderflow() >= 0 {
		b.pager.Unpin(childPage.PageID())
		return false, nil
	}

	return b.merge(p, childPage, childIdx)
}

func (b *Btree) steal(parent *page.Page, childIdx uint16) {
	var leftSibling *page.Page
	if childIdx > 0 {
		leftSibling = b.pager.Get(b.findChildID(parent, childIdx-1))
		defer b.pager.Unpin(leftSibling.PageID())
	}

	var rightSibling *page.Page
	if childIdx < parent.RecordCount() {
		rightSibling = b.pager.Get(b.findChildID(parent, childIdx+1))
		defer b.pager.Unpin(rightSibling.PageID())
	}

	child := b.pager.Get(b.findChildID(parent, childIdx))
	defer b.pager.Unpin(child.PageID())

	for child.BytesUntilUnderflow() < 0 {
		canLeftDonate := leftSibling != nil && leftSibling.BytesUntilUnderflow() >= int(leftSibling.RecordSizeByIndex(leftSibling.RecordCount()-1))
		canRightDonate := rightSibling != nil && rightSibling.BytesUntilUnderflow() >= int(rightSibling.RecordSizeByIndex(0))

		if canLeftDonate {
			b.stealFromLeft(parent, child, leftSibling, childIdx)
		} else if canRightDonate {
			b.stealFromRight(parent, child, rightSibling, childIdx)
		} else {
			break
		}
	}
}

func (b *Btree) stealFromLeft(parent, child, left *page.Page, childIdx uint16) {
	stolenKey := left.KeyByIndex(left.RecordCount() - 1)
	stolenValue := left.ValueByIndex(left.RecordCount() - 1)
	separator := parent.KeyByIndex(childIdx - 1)

	b.pager.MarkDirty(left.PageID())
	b.pager.MarkDirty(child.PageID())
	b.pager.MarkDirty(parent.PageID())

	// left sibling's last record is removed, left sibling's RightPointer becomes that record's value
	left.DeleteRecord(stolenKey)

	if child.PageType() == page.TypeLeaf {
		// target gains the last record of the left sibling
		child.InsertRecord(stolenKey, stolenValue)

		// parent separator updated to the new minimum of target (the stolen key)
		parent.DeleteRecord(separator)
		var leftPointerBin [4]byte
		binary.BigEndian.PutUint32(leftPointerBin[:], left.PageID())
		parent.InsertRecord(stolenKey, leftPointerBin[:])
	}

	if child.PageType() == page.TypeInternal {
		leftNewRightPointer := stolenValue

		// parent separator descends into target as a new first record, with left sibling's RightPointer as its left child value
		var rightPtrBuf [4]byte
		binary.BigEndian.PutUint32(rightPtrBuf[:], left.RightPointer())
		child.InsertRecord(separator, rightPtrBuf[:])

		// left sibling's last key rises to become the new parent separator
		parent.DeleteRecord(separator)
		var leftPointerBin [4]byte
		binary.BigEndian.PutUint32(leftPointerBin[:], left.PageID())
		parent.InsertRecord(stolenKey, leftPointerBin[:])

		// left sibling's RightPointer becomes the stolen record's value
		left.SetRightPointer(binary.BigEndian.Uint32(leftNewRightPointer))
	}
}

func (b *Btree) stealFromRight(parent, child, right *page.Page, childIdx uint16) {
	stolenKey := right.KeyByIndex(0)
	stolenValue := right.ValueByIndex(0)
	separator := parent.KeyByIndex(childIdx)

	b.pager.MarkDirty(right.PageID())
	b.pager.MarkDirty(child.PageID())
	b.pager.MarkDirty(parent.PageID())

	right.DeleteRecord(stolenKey)

	if child.PageType() == page.TypeLeaf {
		// target gains the first record of the right sibling
		child.InsertRecord(stolenKey, stolenValue)

		// parent separator updated to the new minimum of the right sibling
		parent.DeleteRecord(separator)
		newSeparator := right.KeyByIndex(0)
		var childIDBin [4]byte
		binary.BigEndian.PutUint32(childIDBin[:], child.PageID())
		parent.InsertRecord(newSeparator, childIDBin[:])
	}
	if child.PageType() == page.TypeInternal {
		// parent separator descends into target as a new record, with target's current RightPointer as its left child
		var rightPointerBin [4]byte
		binary.BigEndian.PutUint32(rightPointerBin[:], child.RightPointer())
		child.InsertRecord(separator, rightPointerBin[:])

		// target's RightPointer becomes the first record's value of the right sibling
		child.SetRightPointer(binary.BigEndian.Uint32(stolenValue))

		// right sibling's first key rises to become the new parent separator
		parent.DeleteRecord(separator)
		var childIDBin [4]byte
		binary.BigEndian.PutUint32(childIDBin[:], child.PageID())
		parent.InsertRecord(stolenKey, childIDBin[:])
	}
}

func (b *Btree) merge(parent, child *page.Page, childIdx uint16) (bool, error) {
	defer b.pager.Unpin(child.PageID())

	if childIdx > 0 {
		leftSibling := b.pager.Get(b.findChildID(parent, childIdx-1))
		defer b.pager.Unpin(leftSibling.PageID())
		if page.CanMerge(child, leftSibling) {
			return b.mergeWithLeft(parent, child, leftSibling, childIdx)
		}
	}

	if childIdx < parent.RecordCount() {
		rightSibling := b.pager.Get(b.findChildID(parent, childIdx+1))
		defer b.pager.Unpin(rightSibling.PageID())
		if page.CanMerge(child, rightSibling) {
			return b.mergeWithRight(parent, child, rightSibling, childIdx)
		}
	}

	return false, nil
}

func (b *Btree) mergeWithLeft(parent, child, left *page.Page, childIdx uint16) (bool, error) {
	b.pager.MarkDirty(left.PageID())
	b.pager.MarkDirty(parent.PageID())

	leftRightPointer := left.RightPointer()
	parentSepKey := parent.KeyByIndex(childIdx - 1)

	leftRecords := left.ExtractRecords(0, left.RecordCount())
	childRecords := child.ExtractRecords(0, child.RecordCount())
	merged := page.MergeRecords(leftRecords, childRecords)
	// overwrite left in place so its page ID stays valid and the parent pointer remains correct;
	leftPrevLeaf := left.PrevLeaf()
	*left = *page.NewPageFromRecords(left.PageID(), child.PageType(), merged)
	if child.PageType() == page.TypeLeaf {
		left.SetPrevLeaf(leftPrevLeaf)
		b.unlinkLeaf(child)
	}
	b.pager.Free(child.PageID())

	if child.PageType() == page.TypeInternal {
		var leftRightPointerBin [4]byte
		binary.BigEndian.PutUint32(leftRightPointerBin[:], leftRightPointer)
		if err := left.InsertRecord(parentSepKey, leftRightPointerBin[:]); err != nil {
			panic(fmt.Sprintf("unexpected error inserting separator during internal merge: %v", err))
		}
	}

	if childIdx == parent.RecordCount() {
		parent.DeleteRecord(parentSepKey)
		parent.SetRightPointer(left.PageID())
	} else {
		nextSepKey := parent.KeyByIndex(childIdx)
		parent.DeleteRecord(parentSepKey)
		parent.DeleteRecord(nextSepKey)
		var leftIDBin [4]byte
		binary.BigEndian.PutUint32(leftIDBin[:], left.PageID())
		if err := parent.InsertRecord(nextSepKey, leftIDBin[:]); err != nil {
			panic(fmt.Sprintf("unexpected error reinserting separator during merge: %v", err))
		}
	}

	return parent.BytesUntilUnderflow() < 0, nil
}

func (b *Btree) mergeWithRight(parent, child, right *page.Page, childIdx uint16) (bool, error) {
	b.pager.MarkDirty(right.PageID())
	b.pager.MarkDirty(parent.PageID())

	childRightPointer := child.RightPointer()
	sepKey := parent.KeyByIndex(childIdx)

	childRecords := child.ExtractRecords(0, child.RecordCount())
	rightRecords := right.ExtractRecords(0, right.RecordCount())
	merged := page.MergeRecords(childRecords, rightRecords)

	// save NextLeaf first because NewPageFromRecords zeros all header fields
	rightNextLeaf := right.NextLeaf()
	*right = *page.NewPageFromRecords(right.PageID(), child.PageType(), merged)
	if child.PageType() == page.TypeLeaf {
		right.SetNextLeaf(rightNextLeaf)
		b.unlinkLeaf(child)
	}
	b.pager.Free(child.PageID())

	if child.PageType() == page.TypeInternal {
		var childRPBin [4]byte
		binary.BigEndian.PutUint32(childRPBin[:], childRightPointer)
		if err := right.InsertRecord(sepKey, childRPBin[:]); err != nil {
			panic(fmt.Sprintf("unexpected error inserting separator during internal merge: %v", err))
		}
	}

	parent.DeleteRecord(sepKey)

	return parent.BytesUntilUnderflow() < 0, nil
}

// unlinkLeaf removes p from the leaf linked list by joining its neighbors together.
func (b *Btree) unlinkLeaf(p *page.Page) {
	if p.PrevLeaf() != 0 {
		prev := b.pager.Get(p.PrevLeaf())
		prev.SetNextLeaf(p.NextLeaf())
		b.pager.MarkDirty(prev.PageID())
		b.pager.Unpin(prev.PageID())
	} else {
		b.firstLeafID = p.NextLeaf()
	}

	if p.NextLeaf() != 0 {
		next := b.pager.Get(p.NextLeaf())
		next.SetPrevLeaf(p.PrevLeaf())
		b.pager.MarkDirty(next.PageID())
		b.pager.Unpin(next.PageID())
	} else {
		b.lastLeafID = p.PrevLeaf()
	}
}

func (b *Btree) deleteOnLeaf(key []byte, p *page.Page) (bool, error) {
	if !p.DeleteRecord(key) {
		return false, ErrKeyNotFound
	}
	b.pager.MarkDirty(p.PageID())
	return p.BytesUntilUnderflow() < 0, nil
}
