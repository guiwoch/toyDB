package page

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	slotOffsetOff = 0 // Offset of the cell
	slotLengthOff = 2 // Length of the pointed cell
	slotSize      = 4
)

// writeSlot writes a new slot and updates the page header.
func (p *page) writeSlot(cellOffset, cellSize uint16) {
	slotOff := p.slotAlloc()
	binary.BigEndian.PutUint16(p[slotOff+slotOffsetOff:], cellOffset)
	binary.BigEndian.PutUint16(p[slotOff+slotLengthOff:], cellSize)
	p.setSlotAlloc(slotOff + slotSize)
	p.setSlotCount(p.slotCount() + 1)
}

// deleteSlot deletes a slot by index and compacts the slot directory.
func (p *page) deleteSlot(i int) uint16 {
	if i < 0 || i >= int(p.slotCount()) {
		panic(fmt.Sprintf("slot index %d out of bounds [0, %d)", i, p.slotCount()))
	}

	slotOff := pageHeaderSize + i*slotSize
	cellSize := binary.BigEndian.Uint16(p[slotOff+slotLengthOff:])

	isLastSlot := i == int(p.slotCount())-1
	if !isLastSlot { // handles compaction
		copy(p[slotOff:], p[slotOff+slotSize:p.slotAlloc()])
	}

	p.setSlotAlloc(p.slotAlloc() - slotSize)
	p.setSlotCount(p.slotCount() - 1)

	return cellSize
}

func (p *page) updateOffsetSlot(i, offset uint16) {
	slotOffset := pageHeaderSize + i*slotSize
	binary.BigEndian.PutUint16(p[slotOffset:], offset)
}

// getCellOffset returns the cell offset stored in the given slot.
func (p *page) getCellOffset(slotIndex uint16) uint16 {
	slotOff := pageHeaderSize + slotIndex*slotSize
	return binary.BigEndian.Uint16(p[slotOff+slotOffsetOff:])
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
