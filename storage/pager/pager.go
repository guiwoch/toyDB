// Package pager manages page allocation and retrieval
package pager

import "github.com/guiwoch/toyDB/storage/page"

type Pager struct {
	pages   map[uint32]*page.Page
	newID   uint32
	freeIDs []uint32
}

func NewPager() *Pager {
	p := Pager{
		pages: make(map[uint32]*page.Page),
	}
	return &p
}

func (pager *Pager) allocateID() uint32 {
	if len(pager.freeIDs) == 0 {
		r := pager.newID
		pager.newID++
		return r
	}
	r := pager.freeIDs[0]
	pager.freeIDs = pager.freeIDs[1:]
	return r
}

func (pager *Pager) AllocatePage(pageType, keyType uint8) *page.Page {
	id := pager.allocateID()
	newPage := page.NewPage(id, pageType, keyType)

	pager.pages[id] = newPage
	return newPage
}

func (pager *Pager) AllocatePageFromRecords(pageType, keyType uint8, records page.Records) *page.Page {
	id := pager.allocateID()
	newPage := page.NewPageFromRecords(id, pageType, keyType, records)

	pager.pages[id] = newPage
	return newPage
}

func (pager *Pager) FreePage(id uint32) bool {
	_, ok := pager.pages[id]
	if !ok {
		return false
	}
	delete(pager.pages, id)
	pager.freeIDs = append(pager.freeIDs, id)
	return true
}

func (pager *Pager) GetPage(id uint32) *page.Page {
	return pager.pages[id]
}
