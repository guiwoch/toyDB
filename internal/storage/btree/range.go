package btree

import (
	"bytes"
	"iter"

	"github.com/guiwoch/toyDB/internal/storage/page"
)

type Record struct{ Key, Value []byte }

// AscendingRange returns records with keys in [lo, hi), ascending.
// A nil bound means unbounded on that side.
func (b *Btree) AscendingRange(lo, hi []byte) iter.Seq[Record] {
	return func(yield func(Record) bool) {
		var p *page.Page
		if lo == nil { // use the first page
			p = b.pager.Get(b.firstLeafID)
		} else {
			p = b.findLeaf(lo)
		}

		if p.RecordCount() == 0 {
			b.pager.Unpin(p.PageID())
			return
		}

		i, _ := p.SearchKey(lo)

		var key []byte
		for {
			key = p.KeyByIndex(i)
			if hi != nil && bytes.Compare(key, hi) >= 0 {
				break
			}
			value := p.ValueByIndex(i)

			if !yield(Record{Key: key, Value: value}) {
				return
			}

			if i == p.RecordCount()-1 { // no need to worry about hi being nil, this covers it
				if p.NextLeaf() == 0 {
					break
				}

				nextPage := b.pager.Get(p.NextLeaf())
				b.pager.Unpin(p.PageID())
				p = nextPage
				i = 0
			} else {
				i++
			}
		}
		b.pager.Unpin(p.PageID())
	}
}

// DescendingRange returns records with keys in (lo, hi], descending.
// A nil bound means unbounded on that side.
func (b *Btree) DescendingRange(lo, hi []byte) iter.Seq[Record] {
	return func(yield func(Record) bool) {
		var p *page.Page

		if hi == nil { // use the last page
			p = b.pager.Get(b.lastLeafID)
			if p.RecordCount() == 0 {
				b.pager.Unpin(p.PageID())
				return
			}
			hi = p.KeyByIndex(p.RecordCount() - 1)
		} else {
			p = b.findLeaf(hi)
			if p.RecordCount() == 0 {
				b.pager.Unpin(p.PageID())
				return
			}
		}

		i, found := p.SearchKey(hi)
		if !found {
			i--
		}

		for {
			key := p.KeyByIndex(i)
			if lo != nil && bytes.Compare(key, lo) <= 0 {
				break
			}
			value := p.ValueByIndex(i)
			if !yield(Record{Key: key, Value: value}) {
				return
			}

			if i == 0 {
				if p.PrevLeaf() == 0 {
					break
				}

				prevPage := b.pager.Get(p.PrevLeaf())
				b.pager.Unpin(p.PageID())
				p = prevPage
				i = p.RecordCount() - 1
			} else {
				i--
			}
		}
		b.pager.Unpin(p.PageID())
	}
}
