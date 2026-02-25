package btree

import (
	"bytes"

	"github.com/guiwoch/toyDB/storage/page"
)

type Record struct{ Key, Value []byte }

// AscendingRange returns the leaf values [from, to) keys in ascending order.
// Nil is used as the lower and upper bounds
func (b *Btree) AscendingRange(from, to []byte) []Record {
	var records []Record

	var p *page.Page
	if from == nil { // use the first page
		p = b.pager.GetPage(b.firstLeafID)
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

		records = append(records, Record{Key: key, Value: value})

		if i == p.RecordCount()-1 { // no need to worry about to being nil, this covers it
			if p.NextLeaf() == 0 {
				break
			}

			p = b.pager.GetPage(p.NextLeaf())
			i = 0
		} else {
			i++
		}
	}
	return records
}

// DescendingRange returns the leaf values [from, to) keys in ascending order.
// Nil is used as the lower and upper bounds
func (b *Btree) DescendingRange(from, to []byte) []Record {
	var records []Record
	var p *page.Page

	if from == nil { // use the last page
		p = b.pager.GetPage(b.lastLeafID)
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
		records = append(records, Record{Key: key, Value: value})

		if i == 0 {
			if p.PrevLeaf() == 0 {
				break
			}

			p = b.pager.GetPage(p.PrevLeaf())
			i = p.RecordCount() - 1
		} else {
			i--
		}
	}
	return records
}
