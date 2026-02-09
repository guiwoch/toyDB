package page

import (
	"bytes"
	"encoding/binary"
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
	p.setFreeSpace(p.freeSpace() - slotSize)
}


// updateOffsetSlot updates the cell offset stored at slot i.
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
