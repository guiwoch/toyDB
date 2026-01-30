// Package page implements slotted-page storage
package page

const (
	pageSize   = 8192
	headerSize = 128
)

type page [pageSize]byte
