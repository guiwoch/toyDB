# toyDB

An embedded key-value store with typed rows and primary-key indexing,
written in Go for learning purposes.

## When to use this

Only if you are interested in databases and want a simple, hackable
starter library to read or extend;  It is much smaller and simpler
than a real DB, and there is plenty of room to add to it. Or if you
want to evaluate my work.

For anything else, use something real like SQLite.

## Install

```sh
go get github.com/guiwoch/toyDB
```

## Usage

```go
import "github.com/guiwoch/toyDB"

// errors omitted for brevity
d, _ := toydb.Open("example.tdb")
defer d.Close()

s, _ := toydb.NewSchema(0, []toydb.Column{
    {Name: "id",   Type: toydb.TypeInt},
    {Name: "name", Type: toydb.TypeText},
})
t, _ := d.CreateTable("users", s)

t.Insert(toydb.Row{toydb.IntValue(1), toydb.TextValue("guiwoch")})

row, _ := t.Get(toydb.IntValue(1))
```

## What's inside

The on-disk format is a single file of fixed-size 8 KiB pages with a
slotted layout and a per-page checksum verified on every read. A pager
allocates pages, reuses freed ones via an in-page linked freelist, and
acts as a buffer pool that caches hot pages with LRU eviction, pinning
those in active use and flushing dirty pages back to disk before they
leave the cache. Each table is indexed by its primary key through a
B+tree whose leaves are linked both ways for ascending and descending
range scans. A catalog of table definitions, itself a B+tree keyed by
table name, lives in the same file alongside user data, anchored from a
small header in page 0.

## Not yet supported

- Single-process, single-threaded. Not safe for concurrent use.
- No transactions.
- No crash recovery: durability is bounded by `DB.Close`.
- No SQL or query language; the API is methods on `Table`.
- Closed set of column types: `TypeInt` and `TypeText`.

## Documentation

The full API is documented via godoc:

```sh
go doc -all .       # whole package
go doc . DB         # one type
go doc -http        # browse locally with a nice interface
```
