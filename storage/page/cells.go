package page

import (
	"encoding/binary"
)

type cell []byte

const (
	cellKeySizeOff       = 0
	cellValueOrIdSizeOff = 2
	cellHeaderSize       = 4
)

func (p *page) writeCell(key, valueOrID []byte) uint16 {
	keySize, valueOrIDSize := uint16(len(key)), uint16(len(valueOrID))
	cellSize := cellHeaderSize + keySize + valueOrIDSize
	offset := p.cellAlloc() - cellSize

	binary.BigEndian.PutUint16(p[offset+cellKeySizeOff:], keySize)
	binary.BigEndian.PutUint16(p[offset+cellValueOrIdSizeOff:], valueOrIDSize)
	copy(p[offset+cellHeaderSize:], key)
	copy(p[offset+cellHeaderSize+keySize:], valueOrID)

	p.setCellAlloc(offset)
	return offset
}

// getCell returns the entire cell data (header + key + value) at the given slot index.
func (p *page) getCell(slotIndex uint16) []byte {
	cellOffset := p.getCellOffset(slotIndex)
	cellSize := p.getCellSize(slotIndex)
	return p[cellOffset : cellOffset+cellSize]
}

// getCellSize returns the size of the cell at the given slot index.
func (p *page) getCellSize(slotIndex uint16) uint16 {
	slotOff := pageHeaderSize + slotIndex*slotSize
	return binary.BigEndian.Uint16(p[slotOff+slotLengthOff:])
}

func (p *page) compactCells() {
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

func (p *page) cellKey(slotIndex uint16) []byte {
	cellOffset := int(p.getCellOffset(slotIndex))
	keySize := binary.BigEndian.Uint16(p[cellOffset+cellKeySizeOff:])
	keyOffset := cellOffset + cellHeaderSize
	return p[keyOffset : keyOffset+int(keySize)]
}

func (p *page) cellValue(slotIndex uint16) []byte {
	cellOffset := int(p.getCellOffset(slotIndex))
	keySize := binary.BigEndian.Uint16(p[cellOffset+cellKeySizeOff:])
	valueSize := binary.BigEndian.Uint16(p[cellOffset+cellValueOrIdSizeOff:])
	valueOffset := cellOffset + cellHeaderSize + int(keySize)
	return p[valueOffset : valueOffset+int(valueSize)]
}
