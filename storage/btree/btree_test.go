package btree_test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/guiwoch/toyDB/storage/btree"
	"github.com/guiwoch/toyDB/storage/page"
)

type record struct {
	key   [4]byte
	value [16]byte
}

func recordGenerator(n uint32) []record {
	var records []record
	rng := rand.New(rand.NewSource(42))
	for range n {
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], rng.Uint32())

		var value [16]byte
		for i := range 4 {
			copy(value[i*4:], key[:])
		}
		records = append(records, record{key: key, value: value})
	}
	return records
}

func TestInsertAndSearch(t *testing.T) {
	tree := btree.New(page.KeyTypeInt)
	records := recordGenerator(10_000_000)
	for i := range records {
		tree.Insert(records[i].key[:], records[i].value[:])
	}

	for i := range records {
		value, found := tree.Search(records[i].key[:])
		if found == false {
			t.Errorf("expected %v to be present on the tree", records[i].key)
			continue
		}

		if !bytes.Equal(records[i].value[:], value) {
			t.Errorf("expected %v as value but got %v on key %v", records[i].value, value, records[i].key)
		}
	}
}
