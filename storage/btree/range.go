package btree

import (
	"bytes"
	"iter"

	"github.com/guiwoch/toyDB/storage/page"
)

type Record struct{ Key, Value []byte }

// AscendingRange returns the leaf values [from, to) keys in ascending order.
// Nil is used as the lower and upper bounds
func (b *Btree) AscendingRange(from, to []byte) iter.Seq[Record] {
	return func(yield func(Record) bool) {
		var p *page.Page
		if from == nil { // use the first page
			p = b.pager.Get(b.firstLeafID)
		} else {
			p = b.findLeaf(from)
		}

		i, _ := p.SearchKey(from)

		var key []byte
		for {
			key = p.KeyByIndex(i)
			if to != nil && bytes.Compare(key, to) >= 0 {
				break
			}
			value := p.ValueByIndex(i)

			if !yield(Record{Key: key, Value: value}) {
				return
			}

			if i == p.RecordCount()-1 { // no need to worry about to being nil, this covers it
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

// DescendingRange returns the leaf values [from, to) keys in descending order.
// Nil is used as the lower and upper bounds
func (b *Btree) DescendingRange(from, to []byte) iter.Seq[Record] {
	return func(yield func(Record) bool) {
		var p *page.Page

		if from == nil { // use the last page
			p = b.pager.Get(b.lastLeafID)
			from = p.KeyByIndex(p.RecordCount() - 1)
		} else {
			p = b.findLeaf(from)
		}

		i, found := p.SearchKey(from)
		if !found {
			i--
		}

		for {
			key := p.KeyByIndex(i)
			if to != nil && bytes.Compare(key, to) <= 0 {
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
