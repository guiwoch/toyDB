package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	toydb "github.com/guiwoch/toyDB"
	"github.com/guiwoch/toyDB/internal/storage/pager"
)

func main() {
	cacheSize := flag.Int("cache", pager.DefaultCacheSize, "buffer pool size in pages (0 = unlimited)")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "toydb: command-line access to a toyDB database file")
		fmt.Fprintln(os.Stderr, "usage: toydb [flags] [path]")
		flag.PrintDefaults()
	}
	flag.Parse()

	path := "./db.tdb"
	if flag.NArg() == 1 {
		path = flag.Arg(0)
	} else if flag.NArg() > 1 {
		flag.Usage()
		os.Exit(2)
	}

	d, err := toydb.Open(path, toydb.WithCacheSize(*cacheSize))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer d.Close()

	fmt.Fprintf(os.Stdout, "opened %s\n", path)
	repl(d, os.Stdin, os.Stdout)
}

func repl(d *toydb.DB, in io.Reader, out io.Writer) {
	scanner := bufio.NewScanner(in)
	for {
		fmt.Fprint(out, "toydb> ")
		if !scanner.Scan() {
			fmt.Fprintln(out)
			return
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		verb, args := fields[0], fields[1:]

		if verb == "exit" || verb == "quit" {
			return
		}
		if err := dispatch(d, verb, args, out); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
	}
}

func dispatch(d *toydb.DB, verb string, args []string, out io.Writer) error {
	switch verb {
	case "help":
		return cmdHelp(out)
	case "tables":
		return cmdTables(d, out)
	case "create":
		return cmdCreate(d, args)
	case "drop":
		return cmdDrop(d, args)
	case "schema":
		return cmdSchema(d, args, out)
	case "insert":
		return cmdInsert(d, args)
	case "get":
		return cmdGet(d, args, out)
	case "update":
		return cmdUpdate(d, args)
	case "delete":
		return cmdDelete(d, args)
	case "scan":
		return cmdScan(d, args, out)
	case "scandesc":
		return cmdScanDesc(d, args, out)
	default:
		return fmt.Errorf("unknown command %q (try `help`)", verb)
	}
}

func cmdHelp(out io.Writer) error {
	fmt.Fprintln(out, `commands:
  tables                                  list tables
  create <name> <col:int|text|bool|timestamp> ... pk=<col>
                                              example: create users id:int name:text pk=id
                                              timestamp values: RFC3339 (e.g. 2026-04-30T14:00:00Z) or "now"
                                              bool values: true|false (or 1|0)
  drop <name>                             drop a table
  schema <name>                           show table columns
  insert <table> <val> ...                insert a row
  get <table> <key>                       fetch a row by primary key
  update <table> <val> ...                update an existing row
  delete <table> <key>                    delete a row by primary key
  scan <table> [<lo> <hi>]                range scan, ascending (no bounds = all rows)
  scandesc <table> [<lo> <hi>]            range scan, descending (no bounds = all rows)
  help                                    this message
  exit                                    close and quit`)
	return nil
}

func cmdTables(d *toydb.DB, out io.Writer) error {
	names, err := d.Tables()
	if err != nil {
		return err
	}
	for _, name := range names {
		fmt.Fprintln(out, name)
	}
	return nil
}

func cmdCreate(d *toydb.DB, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: create <name> <col:type> ... pk=<col>")
	}
	name, rest := args[0], args[1:]

	var pkName string
	var columns []toydb.Column
	for _, tok := range rest {
		if pk, ok := strings.CutPrefix(tok, "pk="); ok {
			if pkName != "" {
				return fmt.Errorf("multiple pk= clauses")
			}
			pkName = pk
			continue
		}
		col, err := parseColumn(tok)
		if err != nil {
			return err
		}
		columns = append(columns, col)
	}

	if pkName == "" {
		return fmt.Errorf("missing pk=<col>")
	}
	pkIndex := -1
	for i, c := range columns {
		if c.Name == pkName {
			pkIndex = i
			break
		}
	}
	if pkIndex == -1 {
		return fmt.Errorf("pk %q is not among the columns", pkName)
	}

	s, err := toydb.NewSchema(pkIndex, columns)
	if err != nil {
		return err
	}
	if _, err := d.CreateTable(name, s); err != nil {
		return err
	}
	return nil
}

func cmdDrop(d *toydb.DB, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: drop <name>")
	}
	return d.DropTable(args[0])
}

func cmdSchema(d *toydb.DB, args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: schema <name>")
	}
	t, err := d.OpenTable(args[0])
	if err != nil {
		return err
	}
	s := t.Schema()
	pk := s.PrimaryKey()
	for _, c := range s.Columns() {
		marker := ""
		if c.Name == pk {
			marker = " (pk)"
		}
		fmt.Fprintf(out, "%s: %s%s\n", c.Name, typeName(c.Type), marker)
	}
	return nil
}

func cmdInsert(d *toydb.DB, args []string) error {
	t, row, err := openAndParseRow(d, args, "insert")
	if err != nil {
		return err
	}
	return t.Insert(row)
}

func cmdUpdate(d *toydb.DB, args []string) error {
	t, row, err := openAndParseRow(d, args, "update")
	if err != nil {
		return err
	}
	return t.Update(row)
}

func cmdGet(d *toydb.DB, args []string, out io.Writer) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: get <table> <key>")
	}
	t, err := d.OpenTable(args[0])
	if err != nil {
		return err
	}
	s := t.Schema()
	pkCol := pkColumn(s)
	key, err := parseValue(pkCol, args[1])
	if err != nil {
		return err
	}
	row, err := t.Get(key)
	if err != nil {
		return err
	}
	printRow(out, s, row)
	return nil
}

func cmdDelete(d *toydb.DB, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: delete <table> <key>")
	}
	t, err := d.OpenTable(args[0])
	if err != nil {
		return err
	}
	key, err := parseValue(pkColumn(t.Schema()), args[1])
	if err != nil {
		return err
	}
	return t.Delete(key)
}

func cmdScan(d *toydb.DB, args []string, out io.Writer) error {
	if len(args) != 1 && len(args) != 3 {
		return fmt.Errorf("usage: scan <table> [<lo> <hi>]")
	}
	t, err := d.OpenTable(args[0])
	if err != nil {
		return err
	}
	s := t.Schema()
	var lo, hi toydb.Value
	if len(args) == 3 {
		pkCol := pkColumn(s)
		if lo, err = parseValue(pkCol, args[1]); err != nil {
			return err
		}
		if hi, err = parseValue(pkCol, args[2]); err != nil {
			return err
		}
	}
	for row, err := range t.Scan(lo, hi) {
		if err != nil {
			return err
		}
		printRow(out, s, row)
	}
	return nil
}

func cmdScanDesc(d *toydb.DB, args []string, out io.Writer) error {
	if len(args) != 1 && len(args) != 3 {
		return fmt.Errorf("usage: scandesc <table> [<lo> <hi>]")
	}
	t, err := d.OpenTable(args[0])
	if err != nil {
		return err
	}
	s := t.Schema()
	var lo, hi toydb.Value
	if len(args) == 3 {
		pkCol := pkColumn(s)
		if lo, err = parseValue(pkCol, args[1]); err != nil {
			return err
		}
		if hi, err = parseValue(pkCol, args[2]); err != nil {
			return err
		}
	}
	for row, err := range t.ScanDescending(hi, lo) {
		if err != nil {
			return err
		}
		printRow(out, s, row)
	}
	return nil
}

func openAndParseRow(d *toydb.DB, args []string, verb string) (*toydb.Table, toydb.Row, error) {
	if len(args) < 2 {
		return nil, nil, fmt.Errorf("usage: %s <table> <val> ...", verb)
	}
	t, err := d.OpenTable(args[0])
	if err != nil {
		return nil, nil, err
	}
	cols := t.Schema().Columns()
	vals := args[1:]
	if len(vals) != len(cols) {
		return nil, nil, fmt.Errorf("%s: got %d values, schema has %d columns", verb, len(vals), len(cols))
	}
	row := make(toydb.Row, len(cols))
	for i, c := range cols {
		v, err := parseValue(c, vals[i])
		if err != nil {
			return nil, nil, err
		}
		row[i] = v
	}
	return t, row, nil
}

func pkColumn(s *toydb.Schema) toydb.Column {
	pk := s.PrimaryKey()
	for _, c := range s.Columns() {
		if c.Name == pk {
			return c
		}
	}
	return toydb.Column{}
}

func parseColumn(token string) (toydb.Column, error) {
	name, ty, ok := strings.Cut(token, ":")
	if !ok {
		return toydb.Column{}, fmt.Errorf("expected name:type, got %q", token)
	}
	switch ty {
	case "int":
		return toydb.Column{Name: name, Type: toydb.TypeInt}, nil
	case "text":
		return toydb.Column{Name: name, Type: toydb.TypeText}, nil
	case "bool":
		return toydb.Column{Name: name, Type: toydb.TypeBool}, nil
	case "timestamp":
		return toydb.Column{Name: name, Type: toydb.TypeTimestamp}, nil
	default:
		return toydb.Column{}, fmt.Errorf("unknown type %q (want int, text, bool, or timestamp)", ty)
	}
}

func parseValue(col toydb.Column, token string) (toydb.Value, error) {
	switch col.Type {
	case toydb.TypeInt:
		var n int64
		if _, err := fmt.Sscan(token, &n); err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		return toydb.IntValue(n), nil
	case toydb.TypeText:
		return toydb.TextValue(token), nil
	case toydb.TypeBool:
		b, err := strconv.ParseBool(token)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		return toydb.BoolValue(b), nil
	case toydb.TypeTimestamp:
		if token == "now" {
			return toydb.TimestampNow(), nil
		}
		t, err := time.Parse(time.RFC3339, token)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		return toydb.TimestampValue(t.UnixNano()), nil
	default:
		return nil, fmt.Errorf("column %q: unsupported type", col.Name)
	}
}

func printRow(out io.Writer, s *toydb.Schema, row toydb.Row) {
	cols := s.Columns()
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s=%v", c.Name, row[i])
	}
	fmt.Fprintln(out, strings.Join(parts, " "))
}

func typeName(t toydb.ColType) string {
	switch t {
	case toydb.TypeInt:
		return "int"
	case toydb.TypeText:
		return "text"
	case toydb.TypeBool:
		return "bool"
	case toydb.TypeTimestamp:
		return "timestamp"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}
