package pager

import (
	"encoding/binary"
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

type Metadata struct {
	NewID       uint32
	RootID      uint32
	FirstLeafID uint32
	LastLeafID  uint32
	PageCount   uint32
}

const (
	metaNewID     = 0  // uint32
	metaRootID    = 4  // uint32
	metaFirstLeaf = 8  // uint32
	metaLastLeaf  = 12 // uint32
	metaPageCount = 16 // uint32
	metaSize      = 20
)

// reads the contents of the zero-page
func readMeta(file *os.File) (Metadata, error) {
	metadataBytes := make([]byte, metaSize)
	_, err := file.ReadAt(metadataBytes, 0)
	if err != nil {
		return Metadata{}, err
	}

	return Metadata{
		NewID:       binary.BigEndian.Uint32(metadataBytes[metaNewID:]),
		RootID:      binary.BigEndian.Uint32(metadataBytes[metaRootID:]),
		FirstLeafID: binary.BigEndian.Uint32(metadataBytes[metaFirstLeaf:]),
		LastLeafID:  binary.BigEndian.Uint32(metadataBytes[metaLastLeaf:]),
		PageCount:   binary.BigEndian.Uint32(metadataBytes[metaPageCount:]),
	}, nil
}

// writes to the zero-page
func writeMeta(file *os.File, meta Metadata) error {
	metadata := make([]byte, metaSize)
	binary.BigEndian.PutUint32(metadata[metaNewID:], meta.NewID)
	binary.BigEndian.PutUint32(metadata[metaRootID:], meta.RootID)
	binary.BigEndian.PutUint32(metadata[metaFirstLeaf:], meta.FirstLeafID)
	binary.BigEndian.PutUint32(metadata[metaLastLeaf:], meta.LastLeafID)
	binary.BigEndian.PutUint32(metadata[metaPageCount:], meta.PageCount)

	_, err := file.WriteAt(metadata, 0)
	return err
}

// buildFreeList reads every page on disk and returns a freeList slice
func buildFreeList(file *os.File, meta Metadata) ([]uint32, error) {
	freeIDs := make([]uint32, 0, meta.PageCount)
	isFreeByte := make([]byte, 1)
	for i := uint32(1); i < meta.PageCount; i++ {
		_, err := file.ReadAt(isFreeByte, int64(page.PageSize*i+page.HdrIsFree))
		if err != nil {
			return nil, err
		}
		if isFreeByte[0] != 0 {
			freeIDs = append(freeIDs, i)
		}
	}
	return freeIDs, nil
}

func (pager *Pager) readPage(id uint32) (*page.Page, error) {
	var p page.Page
	_, err := pager.file.ReadAt(p[:], int64(id)*int64(page.PageSize))
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (pager *Pager) flush() error {
	// sort the dirty pages improves the disk write
	pageIDs := make([]uint32, 0, len(pager.dirty))
	for k := range pager.dirty {
		pageIDs = append(pageIDs, k)
	}
	slices.Sort(pageIDs)

	for _, id := range pageIDs {
		_, err := pager.file.WriteAt(pager.pages[id][:], int64(id)*int64(page.PageSize))
		if err != nil {
			return fmt.Errorf("page flush error: page id %v - %w", id, err)
		}
	}
	return nil
}

func (pager *Pager) Close(rootID, firstLeafID, lastLeafID uint32) error {
	meta := Metadata{
		NewID:       pager.newID,
		RootID:      rootID,
		FirstLeafID: firstLeafID,
		LastLeafID:  lastLeafID,
		PageCount:   pager.newID,
	}
	if err := writeMeta(pager.file, meta); err != nil {
		return err
	}
	if err := pager.flush(); err != nil {
		return err
	}
	return pager.file.Close()
}
