package page

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	TypeInternal = 1
	TypeLeaf     = 2
)

const (
	hdrPageIDOff    = 0  // uint32
	hdrSlotCountOff = 4  // uint16
	hdrSlotAllocOff = 6  // uint16 (first free byte after slot directory, grows ->)
	hdrCellAllocOff = 8  // uint16 (first free byte before cell data, grows <-)
	hdrFreeSpaceOff = 10 // uint16 (total free space)
	hdrPageTypeOff  = 12 // uint8  (internal=1, leaf=2)
	hdrChecksumOff  = 14 // uint32
	hdrRightPointer = 18 // uint32
	hdrNextLeaf     = 22 // uint32
	hdrPrevLeaf     = 26 // uint32
	hdrNextFree     = 30 // uint32 (next pointer when page is on the freelist; undefined otherwise)
)

func (p *Page) PageID() uint32 {
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

func (p *Page) FreeSpace() uint16 {
	return binary.BigEndian.Uint16(p[hdrFreeSpaceOff:])
}

func (p *Page) setFreeSpace(n uint16) {
	binary.BigEndian.PutUint16(p[hdrFreeSpaceOff:], n)
}

func (p *Page) PageType() uint8 {
	return p[hdrPageTypeOff]
}

func (p *Page) setPageType(n uint8) {
	p[hdrPageTypeOff] = n
}

func (p *Page) RightPointer() uint32 {
	return binary.BigEndian.Uint32(p[hdrRightPointer:])
}

func (p *Page) SetRightPointer(n uint32) {
	binary.BigEndian.PutUint32(p[hdrRightPointer:], n)
}

func (p *Page) NextLeaf() uint32 {
	return binary.BigEndian.Uint32(p[hdrNextLeaf:])
}

func (p *Page) SetNextLeaf(n uint32) {
	binary.BigEndian.PutUint32(p[hdrNextLeaf:], n)
}

func (p *Page) PrevLeaf() uint32 {
	return binary.BigEndian.Uint32(p[hdrPrevLeaf:])
}

func (p *Page) SetPrevLeaf(n uint32) {
	binary.BigEndian.PutUint32(p[hdrPrevLeaf:], n)
}

func (p *Page) NextFree() uint32 {
	return binary.BigEndian.Uint32(p[hdrNextFree:])
}

func (p *Page) SetNextFree(n uint32) {
	binary.BigEndian.PutUint32(p[hdrNextFree:], n)
}

func (p *Page) calculateChecksum() uint32 {
	hasher := crc32.NewIEEE()
	hasher.Write(p[0:hdrChecksumOff])
	hasher.Write(p[hdrChecksumOff+4:])
	return hasher.Sum32()
}
