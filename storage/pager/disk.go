package pager

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/guiwoch/toyDB/storage/page"
)

// openFile opens the specified file or creates one if it doesn't exist.
func openFile(filename string) (file *os.File, created bool, err error) {
	// O_EXCL errors if the file exist
	file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if errors.Is(err, os.ErrExist) {
		file, err = os.OpenFile(filename, os.O_RDWR, 0o644)
		return file, false, err
	}
	return file, true, err
}

var ErrChecksumMismatch = errors.New("page checksum mismatch")

func (pager *Pager) readPage(id uint32) (*page.Page, error) {
	var p page.Page
	_, err := pager.file.ReadAt(p[:], int64(id)*int64(page.PageSize))
	if err != nil {
		return nil, err
	}
	if !p.VerifyChecksum() {
		return nil, fmt.Errorf("%w: page id %v", ErrChecksumMismatch, id)
	}
	return &p, nil
}

// Flush writes all dirty pages to disk and fsyncs. Does not reset the dirty set.
func (pager *Pager) Flush() error {
	// sort the dirty pages improves the disk write
	pageIDs := make([]uint32, 0, len(pager.dirty))
	for k := range pager.dirty {
		pageIDs = append(pageIDs, k)
	}
	slices.Sort(pageIDs)

	for _, id := range pageIDs {
		p := pager.pages[id]
		p.SetChecksum()
		_, err := pager.file.WriteAt(p[:], int64(id)*int64(page.PageSize))
		if err != nil {
			return fmt.Errorf("page flush error: page id %v - %w", id, err)
		}
	}
	return pager.file.Sync()
}

// ReadPage0 reads the raw bytes of page 0 into buf. Page 0 is not managed by
// the buffer pool; it stores the DB header.
func (pager *Pager) ReadPage0(buf []byte) error {
	_, err := pager.file.ReadAt(buf, 0)
	return err
}

// WritePage0 writes buf to page 0 and fsyncs. Callers must size buf appropriately.
func (pager *Pager) WritePage0(buf []byte) error {
	_, err := pager.file.WriteAt(buf, 0)
	if err != nil {
		return err
	}
	return pager.file.Sync()
}

// Close closes the underlying file. Callers must Flush and WritePage0 before
// calling Close to guarantee durability.
func (pager *Pager) Close() error {
	return pager.file.Close()
}
