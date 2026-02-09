package page

import (
	"encoding/binary"
	"fmt"
)

const (
	slotOffsetOff = 0 // Offset of the cell
	slotLengthOff = 2 // Length of the pointed cell
	slotSize      = 4
)

// writeSlot writes a new slot at position i, shifting subsequent slots right.
func (p *Page) writeSlot(cellOffset, cellSize, i uint16) {
	if i > p.slotCount() {
		panic(fmt.Sprintf("slot index %d out of bounds [0, %d]", i, p.slotCount()))
	}
	slotOff := pageHeaderSize + i*slotSize
	end := p.slotAlloc()

	if slotOff < end {
		copy(p[slotOff+slotSize:], p[slotOff:end])
	}

	binary.BigEndian.PutUint16(p[slotOff+slotOffsetOff:], cellOffset)
	binary.BigEndian.PutUint16(p[slotOff+slotLengthOff:], cellSize)
	p.setSlotAlloc(end + slotSize)
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
