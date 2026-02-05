package page

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	KeyTypeInt    = 1
	KeyTypeString = 2
)

const (
	PageTypeNode = 1
	PageTypeLeaf = 2
)

const (
	hdrPageIDOff    = 0  // uint32
	hdrSlotCountOff = 4  // uint16
	hdrSlotAllocOff = 6  // uint16 (first free byte after slot directory, grows ->)
	hdrCellAllocOff = 8  // uint16 (first free byte before cell data, grows <-)
	hdrFreeSpaceOff = 10 // uint16 (total free space)
	hdrPageTypeOff  = 12 // uint8  (leaf=1, internal=2)
	hdrKeyTypeOff   = 13 // uint8  (int=1, string=2)
	hdrChecksumOff  = 14 // uint32
)

func (p *Page) pageID() uint32 {
	return binary.BigEndian.Uint32(p[hdrPageIDOff:])
}

func (p *Page) setPageID(id uint32) {
	binary.BigEndian.PutUint32(p[hdrPageIDOff:], id)
}

func (p *Page) slotCount() uint16 {
	return binary.BigEndian.Uint16(p[hdrSlotCountOff:])
}

func (p *Page) setSlotCount(n uint16) {
	binary.BigEndian.PutUint16(p[hdrSlotCountOff:], n)
}

func (p *Page) slotAlloc() uint16 {
	return binary.BigEndian.Uint16(p[hdrSlotAllocOff:])
}

func (p *Page) setSlotAlloc(n uint16) {
	binary.BigEndian.PutUint16(p[hdrSlotAllocOff:], n)
}

func (p *Page) cellAlloc() uint16 {
	return binary.BigEndian.Uint16(p[hdrCellAllocOff:])
}

func (p *Page) setCellAlloc(n uint16) {
	binary.BigEndian.PutUint16(p[hdrCellAllocOff:], n)
}

func (p *Page) freeSpace() uint16 {
	return binary.BigEndian.Uint16(p[hdrFreeSpaceOff:])
}

func (p *Page) setFreeSpace(n uint16) {
	binary.BigEndian.PutUint16(p[hdrFreeSpaceOff:], n)
}

func (p *Page) pageType() uint8 {
	return p[hdrPageTypeOff]
}

func (p *Page) setPageType(n uint8) {
	p[hdrPageTypeOff] = n
}

func (p *Page) keyType() uint8 {
	return p[hdrKeyTypeOff]
}

func (p *Page) setKeyType(t uint8) {
	p[hdrKeyTypeOff] = t
}

func (p *Page) calculateChecksum() uint32 {
	hasher := crc32.NewIEEE()
	hasher.Write(p[0:hdrChecksumOff])
	hasher.Write(p[hdrChecksumOff+4:])
	return hasher.Sum32()
}
