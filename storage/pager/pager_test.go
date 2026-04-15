package pager

import (
	"errors"
	"os"
	"testing"

	"github.com/guiwoch/toyDB/storage/page"
)

func TestChecksumDetectsCorruption(t *testing.T) {
	path := t.TempDir() + "/test"

	p, _, err := New(page.KeyTypeInt, path)
	if err != nil {
		t.Fatal(err)
	}
	pg := p.Allocate(page.TypeLeaf)
	id := pg.PageID()
	if err := pg.InsertRecord([]byte{0, 0, 0, 1}, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(id, id, id); err != nil {
		t.Fatal(err)
	}

	// Flip a byte inside page 1's payload (past the 64-byte header).
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(page.PageSize)*int64(id) + int64(page.PageHeaderSize) + 16
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, offset); err != nil {
		t.Fatal(err)
	}
	buf[0] ^= 0xFF
	if _, err := f.WriteAt(buf, offset); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and read the corrupted page.
	p2, _, err := New(page.KeyTypeInt, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.readPage(id); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}
