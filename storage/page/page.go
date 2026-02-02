// Package page implements slotted-page storage
package page

const (
	pageSize   = 8192 // 8KB
	headerSize = 64   // Includes reseverd space for future expansions
	pageSize       = 8192 // 8KB
	pageHeaderSize = 64   // Includes reseverd space for future expansions
)

type page [pageSize]byte

func NewPage(id uint32, pageType, keyType uint8) *page {
	var p page
	p.setPageID(id)
	p.setSlotCount(0)
	p.setSlotAlloc(pageHeaderSize)
	p.setCellAlloc(pageSize)
	p.setFreeSpace(pageSize - pageHeaderSize)
	p.setPageType(pageType)
	p.setKeyType(keyType)
	p.setChecksum()
	return &p
}
