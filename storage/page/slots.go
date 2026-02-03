package page

import (
	"encoding/binary"
	"fmt"
)

type slot []byte

const (
	slotOffsetOff = 0 // Offset of the cell
	slotLengthOff = 2 // Length of the pointed cell
	slotSize      = 4
)

func (p *page) writeSlot(cellOffset, cellSize uint16) {
	slotOff := p.slotAlloc()
	binary.BigEndian.PutUint16(p[slotOff+slotOffsetOff:], cellOffset)
	binary.BigEndian.PutUint16(p[slotOff+slotLengthOff:], cellSize)
	p.setSlotAlloc(slotOff + slotSize)
	p.setSlotCount(p.slotCount() + 1)
}

func (p *page) deleteSlot(i int) uint16 {
	if i < 0 || i >= int(p.slotCount()) {
		panic(fmt.Sprintf("slot index %d out of bounds [0, %d)", i, p.slotCount()))
	}

	slotOff := pageHeaderSize + i*slotSize
	cellSize := binary.BigEndian.Uint16(p[slotOff+slotLengthOff:])

	isLastSlot := i == int(p.slotCount())-1
	if !isLastSlot {
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
