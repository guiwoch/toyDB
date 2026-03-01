// Package pager manages page allocation and retrieval
package pager

import "github.com/guiwoch/toyDB/storage/page"

type Pager struct {
	pages   map[uint32]*page.Page
	newID   uint32
	freeIDs []uint32
	pins    []uint8
}

func NewPager() *Pager {
	p := Pager{
		pages: make(map[uint32]*page.Page),
		newID: 1, // zero is the null page, IDs start at one.
	}
	return &p
}

// allocateID returns the next available page ID, reusing freed IDs when possible.
// It also grows the pins to hold the new ID and sets its pin count to 1.
func (pager *Pager) allocateID() uint32 {
	var id uint32
	if len(pager.freeIDs) == 0 {
		id = pager.newID
		pager.newID++
	} else {
		id = pager.freeIDs[0]
		pager.freeIDs = pager.freeIDs[1:]
	}
	for uint32(len(pager.pins)) <= id {
		pager.pins = append(pager.pins, 0)
	}
	pager.pins[id] = 1
	return id
}

func (pager *Pager) Allocate(pageType, keyType uint8) *page.Page {
	id := pager.allocateID()
	newPage := page.NewPage(id, pageType, keyType)
	pager.pages[id] = newPage
	return newPage
}

func (pager *Pager) AllocateFromRecords(pageType, keyType uint8, records *page.Records) *page.Page {
	id := pager.allocateID()
	newPage := page.NewPageFromRecords(id, pageType, keyType, records)

	pager.pages[id] = newPage
	return newPage
}

// Free frees the specified ID
func (pager *Pager) Free(id uint32) bool {
	_, ok := pager.pages[id]
	if !ok {
		return false
	}
	delete(pager.pages, id)
	pager.freeIDs = append(pager.freeIDs, id)
	pager.pins[id] = 0
	return true
}

func (pager *Pager) Get(id uint32) *page.Page {
	pager.pins[id]++
	return pager.pages[id]
}

func (pager *Pager) Unpin(id uint32) {
	if pager.pins[id] > 0 {
		pager.pins[id]--
	}
}
