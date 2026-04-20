// Package page implements slotted-page storage
package page

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	PageSize       = 8192 // 8KB
	PageHeaderSize = 64   // Includes reserved space for future expansions
)

type Page [PageSize]byte

func NewPage(id uint32, pageType uint8) *Page {
	var p Page
	p.setPageID(id)
	p.setSlotCount(0)
	p.setSlotAlloc(PageHeaderSize)
	p.setCellAlloc(PageSize)
	p.setFreeSpace(PageSize - PageHeaderSize)
	p.setPageType(pageType)
	return &p
}

type Records struct {
	Slots        []byte
	Cells        []byte
	RightPointer uint32
}

// NewPageFromRecords creates a new page and populates it with the contents from Records.
// The slots need to be properly defragmented while the cells are lazily defragmented by the page.
func NewPageFromRecords(id uint32, pageType uint8, records *Records) *Page {
	var p Page
	p.setPageID(id)

	slotsSize := uint16(len(records.Slots))
	slotCount := slotsSize / slotSize
	p.setSlotAlloc(PageHeaderSize + slotsSize)
	p.setSlotCount(slotCount)

	cellsSize := uint16(len(records.Cells))
	p.setCellAlloc(PageSize - cellsSize)
	p.setFreeSpace(PageSize - (PageHeaderSize + cellsSize + slotsSize))

	p.setPageType(pageType)

	copy(p[PageHeaderSize:], records.Slots)
	copy(p[PageSize-cellsSize:], records.Cells)
	p.SetRightPointer(records.RightPointer)
	return &p
}

// ExtractRecords returns a copy of the slots and cells for the range [from, to).
// Slot offsets are rewritten to point into the cells correctly.
// The source page is not modified.
func (p *Page) ExtractRecords(from, to uint16) *Records {
	if from > to || to > p.slotCount() {
		panic(fmt.Sprintf("bad range [%d, %d) with %d records", from, to, p.slotCount()))
	}

	// First pass: collect cells and their sizes.
	var cells []byte
	var cellSizes []uint16
	for i := from; i < to; i++ {
		cell := p.getCell(i)
		cells = append(cells, cell...)
		cellSizes = append(cellSizes, uint16(len(cell)))
	}

	// Second pass: compute correct page offsets now that totalCellsSize is known.
	totalCellsSize := uint16(len(cells))
	var slots []byte
	bytesBeforeCell := uint16(0)
	for _, size := range cellSizes {
		cellOffset := PageSize - totalCellsSize + bytesBeforeCell
		slot := make([]byte, slotSize)
		binary.BigEndian.PutUint16(slot[slotOffsetOff:], cellOffset)
		binary.BigEndian.PutUint16(slot[slotLengthOff:], size)
		slots = append(slots, slot...)
		bytesBeforeCell += size
	}

	return &Records{
		Slots:        slots,
		Cells:        cells,
		RightPointer: p.RightPointer(),
	}
}

func MergeRecords(left, right *Records) *Records {
	// Adjust left's slot offsets to account for right's cells being appended
	adjustment := uint16(len(right.Cells))
	adjustedSlots := make([]byte, len(left.Slots))
	copy(adjustedSlots, left.Slots)
	for i := 0; i < len(adjustedSlots); i += slotSize {
		off := binary.BigEndian.Uint16(adjustedSlots[i+slotOffsetOff:])
		binary.BigEndian.PutUint16(adjustedSlots[i+slotOffsetOff:], off-adjustment)
	}

	return &Records{
		Slots:        append(adjustedSlots, right.Slots...),
		Cells:        append(left.Cells, right.Cells...),
		RightPointer: right.RightPointer,
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
	i, found := p.SearchKey(key)
	if found {
		return ErrDuplicateKey
	}

	cellSize := cellHeaderSize + uint16(len(key)) + uint16(len(valueOrID))
	recordSize := slotSize + cellSize
	freeContiguosSpace := p.cellAlloc() - p.slotAlloc()
	if recordSize > freeContiguosSpace {
		if recordSize > p.FreeSpace() {
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
	i, found := p.SearchKey(key)
	if !found {
		return false
	}

	slotOff := PageHeaderSize + i*slotSize

	isLastSlot := i == p.slotCount()-1
	if !isLastSlot {
		copy(p[slotOff:], p[slotOff+slotSize:p.slotAlloc()])
	}

	p.setSlotAlloc(p.slotAlloc() - slotSize)
	p.setSlotCount(p.slotCount() - 1)
	cellSize := uint16(cellHeaderSize + len(p.cellKey(i)) + len(p.cellValue(i)))
	p.setFreeSpace(p.FreeSpace() + slotSize + cellSize)
	return true
}

// Get returns the value associated with the given key.
func (p *Page) Get(key []byte) ([]byte, bool) {
	i, ok := p.SearchKey(key)
	if !ok {
		return nil, false
	}

	return p.cellValue(i), true
}

// ValueByIndex returns the value at the given slot index.
// It returns a copy of the key, so its safe to use across page mutations.
func (p *Page) ValueByIndex(slotIndex uint16) []byte {
	if slotIndex >= p.slotCount() {
		panic(fmt.Sprintf("slot index %d out of bounds [0, %d)", slotIndex, p.slotCount()))
	}
	b := p.cellValue(slotIndex)
	value := make([]byte, len(b))
	copy(value, b)
	return value
}

// KeyByIndex returns the key at the given slot index.
// It returns a copy of the key, so its safe to use across page mutations.
func (p *Page) KeyByIndex(slotIndex uint16) []byte {
	if slotIndex >= p.slotCount() {
		panic(fmt.Sprintf("slot index %d out of bounds [0, %d)", slotIndex, p.slotCount()))
	}
	b := p.cellKey(slotIndex)
	key := make([]byte, len(b))
	copy(key, b)
	return key
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

// RecordSizeByIndex returns the full on-page cost of the record at slot index i,
// including both the slot and cell.
func (p *Page) RecordSizeByIndex(i uint16) uint16 {
	slotOff := PageHeaderSize + i*slotSize
	cellSize := binary.BigEndian.Uint16(p[slotOff+slotLengthOff:])
	return slotSize + cellSize
}

// SearchKey returns the position where the key exists or would be inserted to
// maintain sorted order. The bool indicates whether the key was found.
func (p *Page) SearchKey(key []byte) (uint16, bool) {
	n := p.slotCount()
	if n == 0 {
		return 0, false
	}
	left, right := uint16(0), n // [left, right)
	for left < right {
		mid := left + (right-left)/2
		c := bytes.Compare(key, p.cellKey(mid))
		if c == 0 {
			return mid, true
		}
		if c > 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left, false
}

// BytesUntilUnderflow returns the amount until underflow
// if the number is negative, the page is underflowed.
func (p *Page) BytesUntilUnderflow() int {
	return int((PageSize-PageHeaderSize)/2) - int(p.FreeSpace())
}

func CanMerge(a, b *Page) bool {
	return a.BytesUntilUnderflow()+b.BytesUntilUnderflow() <= 0
}
