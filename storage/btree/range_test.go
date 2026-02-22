package btree_test

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/guiwoch/toyDB/storage/btree"
	"github.com/guiwoch/toyDB/storage/page"
)

func TestAscendingRangeFullScan(t *testing.T) {
	t.Parallel()
	const recordCount = 1_000_000
	tree := btree.New(page.KeyTypeInt)
	records := recordGenerator(recordCount)

	var collisionCount int
	for i := range records {
		err := tree.Insert(records[i].key[:], records[i].value[:])
		if errors.Is(err, page.ErrDuplicateKey) {
			collisionCount++
		}
	}

	rangeResult := tree.AscendingRange(nil, nil)
	if len(rangeResult) != (recordCount - collisionCount) {
		t.Errorf("expected %v records but got %v", recordCount-collisionCount, len(rangeResult))
	}

	for i := range len(rangeResult) - 1 {
		if bytes.Compare(rangeResult[i].Key, rangeResult[i+1].Key) >= 0 {
			t.Errorf("out of order at index %v", i)
		}
	}
}

func TestDescendingRangeFullScan(t *testing.T) {
	t.Parallel()
	const recordCount = 1_000_000
	tree := btree.New(page.KeyTypeInt)
	records := recordGenerator(recordCount)

	var collisionCount int
	for i := range records {
		err := tree.Insert(records[i].key[:], records[i].value[:])
		if errors.Is(err, page.ErrDuplicateKey) {
			collisionCount++
		}
	}

	rangeResult := tree.DescendingRange(nil, nil)
	if len(rangeResult) != (recordCount - collisionCount) {
		t.Errorf("expected %v records but got %v", recordCount-collisionCount, len(rangeResult))
	}

	for i := range len(rangeResult) - 1 {
		if bytes.Compare(rangeResult[i].Key, rangeResult[i+1].Key) < 0 {
			t.Errorf("out of order at index %v", i)
		}
	}
}

var partialScanRecords = []btree.Record{
	{Key: []byte{0, 0, 0, 1}, Value: []byte{0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1}},
	{Key: []byte{0, 0, 0, 3}, Value: []byte{0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 3}},
	{Key: []byte{0, 0, 0, 5}, Value: []byte{0, 0, 0, 5, 0, 0, 0, 5, 0, 0, 0, 5, 0, 0, 0, 5}},
	{Key: []byte{0, 0, 0, 7}, Value: []byte{0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7}},
	{Key: []byte{0, 0, 0, 9}, Value: []byte{0, 0, 0, 9, 0, 0, 0, 9, 0, 0, 0, 9, 0, 0, 0, 9}},
}

func recordsEqual(a, b btree.Record) bool {
	return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value)
}

func assertRangeEqual(t *testing.T, got, want []btree.Record) {
	t.Helper()
	if !slices.EqualFunc(got, want, recordsEqual) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func newPartialScanTree() *btree.Btree {
	tree := btree.New(page.KeyTypeInt)
	for i := range partialScanRecords {
		tree.Insert(partialScanRecords[i].Key, partialScanRecords[i].Value)
	}
	return tree
}

func TestAscendingRangePartialScan(t *testing.T) {
	t.Parallel()
	r1 := partialScanRecords[0] // {1}
	r3 := partialScanRecords[1] // {3}
	r5 := partialScanRecords[2] // {5}
	r7 := partialScanRecords[3] // {7}
	r9 := partialScanRecords[4] // {9}

	t.Run("nil lower bound, existing upper bound", func(t *testing.T) {
		t.Parallel()
		got := newPartialScanTree().AscendingRange(nil, []byte{0, 0, 0, 5})
		assertRangeEqual(t, got, []btree.Record{r1, r3})
	})

	t.Run("existing lower bound, nil upper bound", func(t *testing.T) {
		t.Parallel()
		got := newPartialScanTree().AscendingRange([]byte{0, 0, 0, 5}, nil)
		assertRangeEqual(t, got, []btree.Record{r5, r7, r9})
	})

	t.Run("existing lower and upper bounds", func(t *testing.T) {
		t.Parallel()
		got := newPartialScanTree().AscendingRange([]byte{0, 0, 0, 3}, []byte{0, 0, 0, 7})
		assertRangeEqual(t, got, []btree.Record{r3, r5})
	})

	t.Run("non-existing lower bound", func(t *testing.T) {
		t.Parallel()
		got := newPartialScanTree().AscendingRange([]byte{0, 0, 0, 4}, []byte{0, 0, 0, 8})
		assertRangeEqual(t, got, []btree.Record{r5, r7})
	})

	t.Run("non-existing upper bound", func(t *testing.T) {
		t.Parallel()
		got := newPartialScanTree().AscendingRange([]byte{0, 0, 0, 3}, []byte{0, 0, 0, 6})
		assertRangeEqual(t, got, []btree.Record{r3, r5})
	})
}

func TestDescendingRangePartialScan(t *testing.T) {
	t.Parallel()
	r1 := partialScanRecords[0] // {1}
	r3 := partialScanRecords[1] // {3}
	r5 := partialScanRecords[2] // {5}
	r7 := partialScanRecords[3] // {7}
	r9 := partialScanRecords[4] // {9}

	t.Run("nil upper bound, existing lower bound", func(t *testing.T) {
		t.Parallel()
		got := newPartialScanTree().DescendingRange(nil, []byte{0, 0, 0, 5})
		assertRangeEqual(t, got, []btree.Record{r9, r7})
	})

	t.Run("existing upper bound, nil lower bound", func(t *testing.T) {
		t.Parallel()
		got := newPartialScanTree().DescendingRange([]byte{0, 0, 0, 5}, nil)
		assertRangeEqual(t, got, []btree.Record{r5, r3, r1})
	})

	t.Run("existing upper and lower bounds", func(t *testing.T) {
		t.Parallel()
		got := newPartialScanTree().DescendingRange([]byte{0, 0, 0, 7}, []byte{0, 0, 0, 3})
		assertRangeEqual(t, got, []btree.Record{r7, r5})
	})

	t.Run("non-existing upper bound", func(t *testing.T) {
		t.Parallel()
		got := newPartialScanTree().DescendingRange([]byte{0, 0, 0, 8}, []byte{0, 0, 0, 2})
		assertRangeEqual(t, got, []btree.Record{r7, r5, r3})
	})

	t.Run("non-existing lower bound", func(t *testing.T) {
		t.Parallel()
		got := newPartialScanTree().DescendingRange([]byte{0, 0, 0, 7}, []byte{0, 0, 0, 4})
		assertRangeEqual(t, got, []btree.Record{r7, r5})
	})
}
