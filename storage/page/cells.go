package page

import (
	"encoding/binary"
)

const (
	cellKeySizeOff       = 0
	cellValueOrIdSizeOff = 2
	cellHeaderSize       = 4
)

// writeCell writes a new cell.
// No explicit delete operation is needed: cells without a corresponding slot
// are deleted during compaction.
func (p *Page) writeCell(key, valueOrID []byte) uint16 {
	keySize, valueOrIDSize := uint16(len(key)), uint16(len(valueOrID))
	cellSize := cellHeaderSize + keySize + valueOrIDSize
	offset := p.cellAlloc() - cellSize

	binary.BigEndian.PutUint16(p[offset+cellKeySizeOff:], keySize)
	binary.BigEndian.PutUint16(p[offset+cellValueOrIdSizeOff:], valueOrIDSize)
	copy(p[offset+cellHeaderSize:], key)
	copy(p[offset+cellHeaderSize+keySize:], valueOrID)

	p.setCellAlloc(offset)
	p.setFreeSpace(p.freeSpace() - (cellSize))
	return offset
}

// getCell returns the entire cell data (header + key + value) at the given slot index.
func (p *Page) getCell(slotIndex uint16) []byte {
	cellOffset := p.getCellOffset(slotIndex)
	cellSize := p.getCellSize(slotIndex)
	return p[cellOffset : cellOffset+cellSize]
}

// getCellSize returns the size of the cell at the given slot index.
func (p *Page) getCellSize(slotIndex uint16) uint16 {
	slotOff := pageHeaderSize + slotIndex*slotSize
	return binary.BigEndian.Uint16(p[slotOff+slotLengthOff:])
}

// compactCells compacts the cells and updates the slots cell offsets
func (p *Page) compactCells() {
	n := p.slotCount()
	var cells []byte
	var sizes []uint16

	for i := range n {
		cell := p.getCell(i)
		cells = append(cells, cell...)
		sizes = append(sizes, uint16(len(cell)))
	}

	startOffset := pageSize - len(cells)
	offset := uint16(startOffset)
	for i := range n {
		p.updateOffsetSlot(i, offset)
		offset += sizes[i]
	}

	copy(p[startOffset:], cells)
	p.setCellAlloc(uint16(startOffset))
}

// cellKey returns the key field of a cell
func (p *Page) cellKey(slotIndex uint16) []byte {
	cellOffset := int(p.getCellOffset(slotIndex))
	keySize := binary.BigEndian.Uint16(p[cellOffset+cellKeySizeOff:])
	keyOffset := cellOffset + cellHeaderSize
	return p[keyOffset : keyOffset+int(keySize)]
}

// cellValue returns the value field of a cell
func (p *Page) cellValue(slotIndex uint16) []byte {
	cellOffset := int(p.getCellOffset(slotIndex))
	keySize := binary.BigEndian.Uint16(p[cellOffset+cellKeySizeOff:])
	valueSize := binary.BigEndian.Uint16(p[cellOffset+cellValueOrIdSizeOff:])
	valueOffset := cellOffset + cellHeaderSize + int(keySize)
	return p[valueOffset : valueOffset+int(valueSize)]
}
