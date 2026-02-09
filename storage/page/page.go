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

type Page [pageSize]byte

func NewPage(id uint32, pageType, keyType uint8) *Page {
	var p Page
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
func NewPageFromRecords(id uint32, pageType, keyType uint8, records Records) *Page {
	var p Page
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
func (p *Page) Records() Records {
	slots := p[pageHeaderSize:p.slotAlloc()]
	cells := p[p.cellAlloc():pageSize]
	return Records{
		Slots: slots,
		Cells: cells,
	}
}

var (
	ErrPageFull     = errors.New("insufficient space on page")
	ErrDuplicateKey = errors.New("key already exists")
)

// InsertRecord adds a new key-value pair to the page in sorted order.
// Returns ErrDuplicateKey if the key already exists.
// Returns ErrPageFull if insufficient space even after compaction.
func (p *Page) InsertRecord(key, valueOrID []byte) error {
	i, found := p.findSlot(key)
	if found {
		return ErrDuplicateKey
	}

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
	p.writeSlot(offset, cellSize, i)
	return nil
}

// DeleteRecord deletes the record with the given key and compacts the slot directory.
// Returns false if the key is not found.
func (p *Page) DeleteRecord(key []byte) bool {
	i, found := p.findSlot(key)
	if !found {
		return false
	}

	slotOff := pageHeaderSize + i*slotSize

	isLastSlot := i == p.slotCount()-1
	if !isLastSlot {
		copy(p[slotOff:], p[slotOff+slotSize:p.slotAlloc()])
	}

	p.setSlotAlloc(p.slotAlloc() - slotSize)
	p.setSlotCount(p.slotCount() - 1)
	p.setFreeSpace(p.freeSpace() + slotSize)
	return true
}

// Get returns the value associated with the given key.
func (p *Page) Get(key []byte) ([]byte, bool) {
	i, ok := p.findSlot(key)
	if !ok {
		return nil, false
	}

	return p.cellValue(i), true
}

// VerifyChecksum calculates the page checksum and compares it to the stored one.
func (p *Page) VerifyChecksum() bool {
	stored := binary.BigEndian.Uint32(p[hdrChecksumOff:])
	calculated := p.calculateChecksum()
	return stored == calculated
}

// SetChecksum calculates and stores the page checksum.
// It should be used before writing the page to disk.
func (p *Page) SetChecksum() {
	c := p.calculateChecksum()
	binary.BigEndian.PutUint32(p[hdrChecksumOff:], c)
}

// RecordCount returns the total number of Records on the page.
func (p *Page) RecordCount() uint16 {
	return p.slotCount()
}

func (p *Page) PageType() uint8 {
	return p.pageType()
}
