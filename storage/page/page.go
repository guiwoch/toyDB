// Package page implements slotted-page storage
package page

import (
	"encoding/binary"
	"errors"
)

const (
	pageSize       = 8192 // 8KB
	pageHeaderSize = 64   // Includes reserved space for future expansions
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
	return &p
}

type Records struct {
	Slots []byte
	Cells []byte
}

// NewPageFromRecords creates a new page and populates it with the contents from Records.
// The slots need to be properly defragmented while the cells are lazily defragmented by the page.
func NewPageFromRecords(id uint32, pageType, keyType uint8, records Records) *page {
	var p page
	p.setPageID(id)

	slotsSize := uint16(len(records.Slots))
	slotCount := slotsSize / slotSize
	p.setSlotAlloc(pageHeaderSize + slotsSize)
	p.setSlotCount(slotCount)

	cellsSize := uint16(len(records.Cells))
	p.setCellAlloc(pageSize - cellsSize)
	p.setFreeSpace(pageSize - (pageHeaderSize + cellsSize + slotsSize))

	p.setPageType(pageType)
	p.setKeyType(keyType)

	copy(p[pageHeaderSize:], records.Slots)
	copy(p[pageSize-cellsSize:], records.Cells)
	return &p
}

// Records returns the slots and cells from the page.
func (p *page) Records() Records {
	slots := p[pageHeaderSize:p.slotAlloc()]
	cells := p[p.cellAlloc():pageSize]
	return Records{
		Slots: slots,
		Cells: cells,
	}
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

// DeleteRecord deletes a slot and its associated cell.
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

// VerifyChecksum calculates the page checksum and compares it to the stored one.
func (p *page) VerifyChecksum() bool {
	stored := binary.BigEndian.Uint32(p[hdrChecksumOff:])
	calculated := p.calculateChecksum()
	return stored == calculated
}

// SetChecksum calculates and stores the page checksum.
// It should be used before writing the page to disk.
func (p *page) SetChecksum() {
	c := p.calculateChecksum()
	binary.BigEndian.PutUint32(p[hdrChecksumOff:], c)
}

// RecordCount returns the total number of Records on the page.
func (p *page) RecordCount() uint16 {
	return p.slotCount()
}

func (p *page) PageType() uint8 {
	return p.pageType()
}
