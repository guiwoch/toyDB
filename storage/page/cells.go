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
