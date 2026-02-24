package page_test

import (
	"bytes"
	"testing"

	"github.com/guiwoch/toyDB/storage/page"
)

type record struct {
	key   []byte
	value []byte
}
type records []record

// Creates a page and populates it with records
func newTestPage(t *testing.T, records records) *page.Page {
	t.Helper()
	p := page.NewPage(0, page.TypeInternal, page.KeyTypeInt)
	for _, r := range records {
		err := p.InsertRecord(r.key, r.value)
		if err != nil {
			t.Fatalf("Insertion failed: %v", err)
		}
	}
	return p
}

func TestRecordCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		records records
		want    uint16
	}{
		{"empty page", nil, 0},
		{"one record", records{{[]byte("k"), []byte("v")}}, 1},
		{"three records", records{
			{[]byte("a"), []byte("1")},
			{[]byte("b"), []byte("2")},
			{[]byte("c"), []byte("3")},
		}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := newTestPage(t, tt.records)
			got := p.RecordCount()
			if got != tt.want {
				t.Errorf("RecordCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestGet(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		records   records
		key       []byte
		wantValue []byte
		wantFound bool
	}{
		{
			"Get record 1",
			records{
				{[]byte("a"), []byte("1")},
				{[]byte("b"), []byte("2")},
				{[]byte("c"), []byte("3")},
			},
			[]byte("a"), []byte("1"), true,
		},
		{
			"Get record 2",
			records{
				{[]byte("a"), []byte("1")},
				{[]byte("b"), []byte("2")},
				{[]byte("c"), []byte("3")},
			},
			[]byte("b"), []byte("2"), true,
		},
		{
			"Get non existent record",
			records{
				{[]byte("a"), []byte("1")},
				{[]byte("b"), []byte("2")},
				{[]byte("c"), []byte("3")},
			},
			[]byte("z"), nil, false,
		},
		{
			"Get from single record (underflow risk if bad binary-search)",
			records{
				{[]byte("a"), []byte("1")},
			},
			[]byte("a"), []byte("1"), true,
		},
		{
			"Get from single record (underflow risk if bad binary-search) 2",
			records{
				{[]byte("a"), []byte("1")},
			},
			[]byte("z"), nil, false,
		},
		{
			"Get record from empty page",
			records{},
			[]byte("a"), nil, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := newTestPage(t, tt.records)
			gotValue, gotFound := p.Get(tt.key)
			if !bytes.Equal(gotValue, tt.wantValue) {
				t.Errorf("Get() value = %v, want %v", gotValue, tt.wantValue)
			}
			if gotFound != tt.wantFound {
				t.Errorf("Get() found = %v, want %v", gotFound, tt.wantFound)
			}
		})
	}
}

func TestDeleteRecord(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		records records
		key     []byte
	}{
		{
			"Case1",
			records{
				{[]byte("a"), []byte("1")},
				{[]byte("b"), []byte("2")},
				{[]byte("c"), []byte("3")},
			},
			[]byte("a"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := newTestPage(t, tt.records)
			ok := p.DeleteRecord(tt.key)
			if !ok {
				t.Error("DeleteRecord key not found")
			}
			if _, found := p.Get(tt.key); found {
				t.Errorf("Deleted key still found")
			}
		})
	}
}
