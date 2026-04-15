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
}

// Open opens (or creates) a pager-backed file. wasFresh reports whether the
// file was created by this call. Page 0 is reserved for the DB header and is
// not managed by the buffer pool.
func Open(filename string) (*Pager, bool, error) {
	file, created, err := openFile(filename)
	if err != nil {
		return nil, false, err
	}
	if created {
		return &Pager{
			pages: make(map[uint32]*page.Page),
			dirty: make(map[uint32]struct{}),
			file:  file,
			newID: 1,
		}, true, nil
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, false, err
	}
	newID := uint32(stat.Size() / int64(page.PageSize))
	if newID == 0 {
		newID = 1
	}
	freeList, err := buildFreeList(file, newID)
	if err != nil {
		file.Close()
		return nil, false, err
	}
	return &Pager{
		freeIDs: freeList,
		pages:   make(map[uint32]*page.Page),
		dirty:   make(map[uint32]struct{}),
		file:    file,
		newID:   newID,
	}, false, nil
}

// NewID returns the next page ID that would be allocated from fresh space
// (excluding the free list). DB persists this in page 0 on close.
func (pager *Pager) NewID() uint32 {
	return pager.newID
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
	pager.dirty[id] = struct{}{}
	return newPage
}

func (pager *Pager) AllocateFromRecords(pageType, keyType uint8, records *page.Records) *page.Page {
	id := pager.allocateID()
	newPage := page.NewPageFromRecords(id, pageType, keyType, records)
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
