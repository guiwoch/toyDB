// Package pager manages page allocation and retrieval
package pager

import (
	"fmt"
	"os"

	"github.com/guiwoch/toyDB/storage/page"
)

type Pager struct {
	pins         []uint8
	pages        map[uint32]*page.Page
	dirty        map[uint32]struct{}
	file         *os.File
	newID        uint32
	freeListHead uint32
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
	return &Pager{
		pages: make(map[uint32]*page.Page),
		dirty: make(map[uint32]struct{}),
		file:  file,
		newID: newID,
	}, false, nil
}

// FreeListHead returns the head of the on-disk freelist.
func (pager *Pager) FreeListHead() uint32 { return pager.freeListHead }

// SetFreeListHead seeds the freelist head from a persisted value (typically
// the DB header in page 0). Call once on Open for an existing file.
func (pager *Pager) SetFreeListHead(h uint32) { pager.freeListHead = h }

// NewID returns the next page ID that would be allocated from fresh space
// (excluding the free list). DB persists this in page 0 on close.
func (pager *Pager) NewID() uint32 {
	return pager.newID
}

// allocateID returns the next available page ID, popping from the freelist
// LIFO when possible. It grows the pins slice to hold the new ID and sets its
// pin count to 1.
func (pager *Pager) allocateID() uint32 {
	var id uint32
	if pager.freeListHead == 0 {
		id = pager.newID
		pager.newID++
	} else {
		id = pager.freeListHead
		next, err := pager.peekNextFree(id)
		if err != nil {
			panic(fmt.Sprintf("pager: walking freelist at page %d: %v", id, err))
		}
		pager.freeListHead = next
	}
	for uint32(len(pager.pins)) <= id {
		pager.pins = append(pager.pins, 0)
	}
	pager.pins[id] = 1
	return id
}

// peekNextFree reads the NextFree pointer of a page on the freelist, faulting
// it in from disk if needed. The page is not added to the cache because the
// caller is about to reinitialize it.
func (pager *Pager) peekNextFree(id uint32) (uint32, error) {
	if cached, ok := pager.pages[id]; ok {
		return cached.NextFree(), nil
	}
	pg, err := pager.readPage(id)
	if err != nil {
		return 0, err
	}
	return pg.NextFree(), nil
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

// Free pushes the page onto the freelist so its ID can be reused. The page
// stays in memory and dirty so its NextFree pointer reaches disk on flush.
func (pager *Pager) Free(id uint32) bool {
	p, ok := pager.pages[id]
	if !ok {
		return false
	}
	p.SetNextFree(pager.freeListHead)
	pager.freeListHead = id
	pager.dirty[id] = struct{}{}
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
