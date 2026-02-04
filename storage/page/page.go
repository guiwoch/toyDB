// Package page implements slotted-page storage
package page

import (
	"bytes"
	"errors"
)

const (
	pageSize       = 8192 // 8KB
	pageHeaderSize = 64   // Includes reseverd space for future expansions
)

type page [pageSize]byte

func NewPage(id uint32, pageType, keyType uint8) *page {
	var p page
	p.setPageID(id)
	p.setSlotCount(0)
	p.setSlotAlloc(pageHeaderSize)
	p.setCellAlloc(pageSize)
	p.setFreeSpace(pageSize - pageHeaderSize)
	p.setPageType(pageType)
	p.setKeyType(keyType)
	p.setChecksum()
	return &p
}

var ErrPageFull = errors.New("insufficient space on page")

// InsertRecord adds a new key-value pair to the page.
// Returns ErrPageFull if insufficient space even after compaction.
func (p *page) InsertRecord(key, valueOrID []byte) error {
	cellSize := cellHeaderSize + uint16(len(key)) + uint16(len(valueOrID))
	recordSize := slotSize + cellSize
	freeContiguosSpace := p.cellAlloc() - p.slotAlloc()
	if recordSize > freeContiguosSpace {
		if recordSize > p.freeSpace() {
			return ErrPageFull
		} else {
			p.compactCells()
		}
	}

	offset := p.writeCell(key, valueOrID)
	p.writeSlot(offset, cellSize)
	return nil
}

func (p *page) DeleteRecord(slotIndex int) {
	cellSize := p.deleteSlot(slotIndex)
	p.setFreeSpace(p.freeSpace() + slotSize + cellSize)
}

// Get returns the value associated with the given key.
func (p *page) Get(key []byte) ([]byte, bool) {
	i, ok := p.findSlot(key)
	if !ok {
		return []byte(""), false
	}

	return p.cellValue(i), true
}

// findSlot returns the slot index for the given key.
func (p *page) findSlot(key []byte) (uint16, bool) {
	left := uint16(0)
	n := p.slotCount()
	if n <= 0 {
		return 0, false
	}
	right := n - 1

	for left <= right {
		mid := (left + right) / 2
		c := bytes.Compare(key, p.cellKey(mid))
		if c == 0 {
			return mid, true
		}
		if c == 1 { // the key is bigger than the midpoint
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return 0, false
}
