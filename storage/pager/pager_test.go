package pager

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"

	"github.com/guiwoch/toyDB/storage/page"
)

func TestChecksumDetectsCorruption(t *testing.T) {
	path := t.TempDir() + "/test"

	p, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	pg := p.Allocate(page.TypeLeaf, page.KeyTypeInt)
	id := pg.PageID()
	if err := pg.InsertRecord([]byte{0, 0, 0, 1}, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Flip a byte inside the page's payload (past the 64-byte header).
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
	p2, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.readPage(id); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}

func allocID(t *testing.T, p *Pager) uint32 {
	t.Helper()
	return p.Allocate(page.TypeLeaf, page.KeyTypeInt).PageID()
}

func TestFreelistReusesIDsLIFO(t *testing.T) {
	p, _, err := Open(t.TempDir() + "/test")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	a := allocID(t, p)
	b := allocID(t, p)
	c := allocID(t, p)

	p.Free(a)
	p.Free(b)
	// head = b, b.next = a

	if got := allocID(t, p); got != b {
		t.Errorf("expected reuse of %d (LIFO), got %d", b, got)
	}
	if got := allocID(t, p); got != a {
		t.Errorf("expected reuse of %d after draining, got %d", a, got)
	}
	if got := allocID(t, p); got != c+1 {
		t.Errorf("expected fresh id %d after freelist drained, got %d", c+1, got)
	}
}

func TestFreelistSurvivesReopen(t *testing.T) {
	path := t.TempDir() + "/test"

	p, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a := allocID(t, p)
	b := allocID(t, p)
	c := allocID(t, p)
	p.Free(a)
	p.Free(c)
	// head = c, c.next = a
	head := p.FreeListHead()
	if head != c {
		t.Fatalf("expected head=%d before close, got %d", c, head)
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	// Store the head in a dedicated byte of page 0 (simulating what DB does).
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], head)
	if err := p.WritePage0(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if err := p2.ReadPage0(hdr[:]); err != nil {
		t.Fatal(err)
	}
	p2.SetFreeListHead(binary.BigEndian.Uint32(hdr[:]))

	if got := allocID(t, p2); got != c {
		t.Errorf("expected reuse of %d after reopen, got %d", c, got)
	}
	if got := allocID(t, p2); got != a {
		t.Errorf("expected reuse of %d after reopen, got %d", a, got)
	}
	// freelist drained; next allocation should be fresh beyond b.
	if got := allocID(t, p2); got <= b {
		t.Errorf("expected fresh id > %d, got %d", b, got)
	}
	_ = b
}
