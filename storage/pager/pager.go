// Package pager manages page allocation and retrieval
package pager

import (
	"os"

	"github.com/guiwoch/toyDB/storage/page"
)

type Pager struct {
	freeIDs []uint32
	pins    []uint8
	pages   map[uint32]*page.Page
	dirty   map[uint32]struct{}
	file    *os.File
	newID   uint32
	keyType uint8
}

func New(keyType uint8, filename string) (*Pager, Metadata, error) {
	file, created, err := openFile(filename)
	if err != nil {
		return nil, Metadata{}, err
	}
	if created {
		p := &Pager{
			pages:   make(map[uint32]*page.Page),
			dirty:   make(map[uint32]struct{}),
			file:    file,
			newID:   1, // zero is the null page, IDs start at one.
			keyType: keyType,
		}
		return p, Metadata{}, nil
	}
	meta, err := readMeta(file)
	if err != nil {
		return nil, Metadata{}, err
	}
	freeList, err := buildFreeList(file, meta)
	if err != nil {
		return nil, Metadata{}, err
	}
	p := &Pager{
		freeIDs: freeList,
		pages:   make(map[uint32]*page.Page),
		dirty:   make(map[uint32]struct{}),
		file:    file,
		newID:   meta.NewID,
		keyType: keyType,
	}
	return p, meta, nil
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

func (pager *Pager) Allocate(pageType uint8) *page.Page {
	id := pager.allocateID()
	newPage := page.NewPage(id, pageType, pager.keyType)
	pager.pages[id] = newPage
	pager.dirty[id] = struct{}{}
	return newPage
}

func (pager *Pager) AllocateFromRecords(pageType uint8, records *page.Records) *page.Page {
	id := pager.allocateID()
	newPage := page.NewPageFromRecords(id, pageType, pager.keyType, records)
	pager.pages[id] = newPage
	pager.dirty[id] = struct{}{}
	return newPage
}

// Free marks the page as free and recycles its ID for future allocations.
// The page stays in memory and dirty so its isFree flag gets written to disk on flush.
func (pager *Pager) Free(id uint32) bool {
	p, ok := pager.pages[id]
	if !ok {
		return false
	}
	p.SetFree()
	pager.dirty[id] = struct{}{}
	pager.freeIDs = append(pager.freeIDs, id)
	pager.pins[id] = 0
	return true
}

func (pager *Pager) Get(id uint32) *page.Page {
	if _, ok := pager.pages[id]; !ok {
		p, err := pager.readPage(id)
		if err != nil {
			panic(err)
		}
		pager.pages[id] = p
	}
	for uint32(len(pager.pins)) <= id {
		pager.pins = append(pager.pins, 0)
	}
	pager.pins[id]++
	return pager.pages[id]
}

func (pager *Pager) Unpin(id uint32) {
	if pager.pins[id] > 0 {
		pager.pins[id]--
	}
}

func (pager *Pager) MarkDirty(id uint32) {
	pager.dirty[id] = struct{}{}
}
