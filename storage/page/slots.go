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
func (p *Page) writeSlot(cellOffset, cellSize uint16) {
	slotOff := p.slotAlloc()
	binary.BigEndian.PutUint16(p[slotOff+slotOffsetOff:], cellOffset)
	binary.BigEndian.PutUint16(p[slotOff+slotLengthOff:], cellSize)
	p.setSlotAlloc(slotOff + slotSize)
	p.setSlotCount(p.slotCount() + 1)
}

// deleteSlot deletes a slot by index and compacts the slot directory.
func (p *Page) deleteSlot(i uint16) uint16 {
	if i >= p.slotCount() {
		panic(fmt.Sprintf("slot index %d out of bounds [0, %d)", i, p.slotCount()))
	}

	slotOff := pageHeaderSize + i*slotSize
	cellSize := binary.BigEndian.Uint16(p[slotOff+slotLengthOff:])

	isLastSlot := i == p.slotCount()-1
	if !isLastSlot { // handles compaction
		copy(p[slotOff:], p[slotOff+slotSize:p.slotAlloc()])
	}

	p.setSlotAlloc(p.slotAlloc() - slotSize)
	p.setSlotCount(p.slotCount() - 1)

	return cellSize
}

func (p *Page) updateOffsetSlot(i, offset uint16) {
	slotOffset := pageHeaderSize + i*slotSize
	binary.BigEndian.PutUint16(p[slotOffset:], offset)
}

// getCellOffset returns the cell offset stored in the given slot.
func (p *Page) getCellOffset(slotIndex uint16) uint16 {
	slotOff := pageHeaderSize + slotIndex*slotSize
	return binary.BigEndian.Uint16(p[slotOff+slotOffsetOff:])
}

// findSlot returns the slot index for the given key.
func (p *Page) findSlot(key []byte) (uint16, bool) {
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
	return 0, false
}
