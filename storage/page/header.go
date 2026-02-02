package page

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	keyTypeInt    = 1
	keyTypeString = 2
)

const (
	pageTypeNode = 1
	pageTypeLeaf = 2
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

func (p *page) pageID() uint32 {
	return binary.BigEndian.Uint32(p[hdrPageIDOff:])
}

func (p *page) setPageID(id uint32) {
	binary.BigEndian.PutUint32(p[hdrPageIDOff:], id)
}

func (p *page) slotCount() uint16 {
	return binary.BigEndian.Uint16(p[hdrSlotCountOff:])
}

func (p *page) setSlotCount(n uint16) {
	binary.BigEndian.PutUint16(p[hdrSlotCountOff:], n)
}

func (p *page) slotAlloc() uint16 {
	return binary.BigEndian.Uint16(p[hdrSlotAllocOff:])
}

func (p *page) setSlotAlloc(n uint16) {
	binary.BigEndian.PutUint16(p[hdrSlotAllocOff:], n)
}

func (p *page) cellAlloc() uint16 {
	return binary.BigEndian.Uint16(p[hdrCellAllocOff:])
}

func (p *page) setCellAlloc(n uint16) {
	binary.BigEndian.PutUint16(p[hdrCellAllocOff:], n)
}

func (p *page) freeSpace() uint16 {
	return binary.BigEndian.Uint16(p[hdrFreeSpaceOff:])
}

func (p *page) setFreeSpace(n uint16) {
	binary.BigEndian.PutUint16(p[hdrFreeSpaceOff:], n)
}

func (p *page) pageType() uint8 {
	return p[hdrPageTypeOff]
}

func (p *page) setPageType(n uint8) {
	p[hdrPageTypeOff] = n
}

func (p *page) keyType() uint8 {
	return p[hdrKeyTypeOff]
}

func (p *page) setKeyType(t uint8) {
	p[hdrKeyTypeOff] = t
}

func (p *page) checksum() uint32 {
	return binary.LittleEndian.Uint32(p[hdrChecksumOff:])
}

func (p *page) setChecksum() {
	c := p.calculateChecksum()
	binary.LittleEndian.PutUint32(p[hdrChecksumOff:], c)
}

func (p *page) verifyCheckSum() bool {
	return p.checksum() == p.calculateChecksum()
}

func (p *page) calculateChecksum() uint32 {
	checksum := crc32.NewIEEE()
	checksum.Write(p[0:hdrChecksumOff])
	checksum.Write(p[hdrChecksumOff+4:])
	return checksum.Sum32()
}
