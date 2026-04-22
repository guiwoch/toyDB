// Package pager manages page allocation and retrieval
package pager

import (
	"container/list"
	"fmt"
	"os"

	"github.com/guiwoch/toyDB/storage/page"
)

// DefaultCacheSize caps the buffer pool at 1024 pages (~8 MiB) by default.
const DefaultCacheSize = 1024

type Pager struct {
	pins         []uint8
	pages        map[uint32]*page.Page
	dirty        map[uint32]struct{}
	file         *os.File
	newID        uint32
	freeListHead uint32

	cacheCap    int
	lru         *list.List
	lruNodes    map[uint32]*list.Element
	pinnedCount int
}

// Option configures optional Pager behavior.
type Option func(*Pager)

// WithCacheSize sets the maximum number of pages held in the buffer pool.
// A value of 0 disables the cap.
func WithCacheSize(n int) Option {
	return func(p *Pager) { p.cacheCap = n }
}

// Open opens (or creates) a pager-backed file. wasFresh reports whether the
// file was created by this call. Page 0 is reserved for the DB header and is
// not managed by the buffer pool.
func Open(filename string, opts ...Option) (*Pager, bool, error) {
	file, created, err := openFile(filename)
	if err != nil {
		return nil, false, err
	}
	newID := uint32(1)
	if !created {
		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, false, err
		}
		if n := uint32(stat.Size() / int64(page.PageSize)); n > 1 {
			newID = n
		}
	}
	p := &Pager{
		pages:    make(map[uint32]*page.Page),
		dirty:    make(map[uint32]struct{}),
		file:     file,
		newID:    newID,
		cacheCap: DefaultCacheSize,
		lru:      list.New(),
		lruNodes: make(map[uint32]*list.Element),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, created, nil
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
		// If the freed page was still in cache, drop it before reinitialization.
		pager.dropFromCache(id)
	}
	for uint32(len(pager.pins)) <= id {
		pager.pins = append(pager.pins, 0)
	}
	pager.pins[id] = 1
	pager.pinnedCount++
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

func (pager *Pager) Allocate(pageType uint8) *page.Page {
	if err := pager.evictIfNeeded(); err != nil {
		panic(err)
	}
	id := pager.allocateID()
	newPage := page.NewPage(id, pageType)
	pager.pages[id] = newPage
	pager.dirty[id] = struct{}{}
	return newPage
}

func (pager *Pager) AllocateFromRecords(pageType uint8, records *page.Records) *page.Page {
	if err := pager.evictIfNeeded(); err != nil {
		panic(err)
	}
	id := pager.allocateID()
	newPage := page.NewPageFromRecords(id, pageType, records)
	pager.pages[id] = newPage
	pager.dirty[id] = struct{}{}
	return newPage
}

// Free pushes the page onto the freelist so its ID can be reused. The page
// stays in memory and dirty so its NextFree pointer reaches disk on flush;
// because its pin is cleared, the LRU may evict it before flush.
func (pager *Pager) Free(id uint32) bool {
	p, ok := pager.pages[id]
	if !ok {
		return false
	}
	p.SetNextFree(pager.freeListHead)
	pager.freeListHead = id
	pager.dirty[id] = struct{}{}
	if pager.pins[id] > 0 {
		pager.pinnedCount--
	}
	pager.pins[id] = 0
	if _, inLRU := pager.lruNodes[id]; !inLRU {
		pager.lruNodes[id] = pager.lru.PushBack(id)
	}
	return true
}

func (pager *Pager) Get(id uint32) *page.Page {
	if pg, ok := pager.pages[id]; ok {
		if elem, inLRU := pager.lruNodes[id]; inLRU {
			pager.lru.Remove(elem)
			delete(pager.lruNodes, id)
		}
		if pager.pins[id] == 0 {
			pager.pinnedCount++
		}
		pager.pins[id]++
		return pg
	}
	if err := pager.evictIfNeeded(); err != nil {
		panic(err)
	}
	pg, err := pager.readPage(id)
	if err != nil {
		panic(err)
	}
	pager.pages[id] = pg
	for uint32(len(pager.pins)) <= id {
		pager.pins = append(pager.pins, 0)
	}
	pager.pins[id] = 1
	pager.pinnedCount++
	return pg
}

func (pager *Pager) Unpin(id uint32) {
	if pager.pins[id] == 0 {
		return
	}
	pager.pins[id]--
	if pager.pins[id] == 0 {
		pager.pinnedCount--
		if _, inLRU := pager.lruNodes[id]; !inLRU {
			pager.lruNodes[id] = pager.lru.PushBack(id)
		}
	}
}

func (pager *Pager) MarkDirty(id uint32) {
	pager.dirty[id] = struct{}{}
}

// evictIfNeeded drops LRU-front pages until the cache is below the cap.
// Pinned pages aren't in the LRU and can't be evicted.
func (pager *Pager) evictIfNeeded() error {
	if pager.cacheCap <= 0 {
		return nil
	}
	for len(pager.pages) >= pager.cacheCap {
		elem := pager.lru.Front()
		if elem == nil {
			return nil // everything pinned; can't evict
		}
		id := elem.Value.(uint32)
		if err := pager.evictPage(id, elem); err != nil {
			return err
		}
	}
	return nil
}

// evictPage flushes a dirty page before dropping it from the cache.
// Does not fsync — Flush is the sync point.
func (pager *Pager) evictPage(id uint32, elem *list.Element) error {
	pager.lru.Remove(elem)
	delete(pager.lruNodes, id)
	if _, isDirty := pager.dirty[id]; isDirty {
		pg := pager.pages[id]
		pg.SetChecksum()
		if _, err := pager.file.WriteAt(pg[:], int64(id)*int64(page.PageSize)); err != nil {
			return fmt.Errorf("evict page %d: %w", id, err)
		}
		delete(pager.dirty, id)
	}
	delete(pager.pages, id)
	return nil
}

// PinnedCount returns the number of pages currently pinned.
func (pager *Pager) PinnedCount() int { return pager.pinnedCount }

// dropFromCache removes an id from pages/dirty/lru without writing.
// Used when allocateID is about to reinitialize the page.
func (pager *Pager) dropFromCache(id uint32) {
	if elem, inLRU := pager.lruNodes[id]; inLRU {
		pager.lru.Remove(elem)
		delete(pager.lruNodes, id)
	}
	delete(pager.pages, id)
	delete(pager.dirty, id)
}
