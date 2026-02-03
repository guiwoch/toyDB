// Package page implements slotted-page storage
package page

import (
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
