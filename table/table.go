package table

import (
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/jinpan/stuffdb/column"
	"github.com/jinpan/stuffdb/schema"
	"github.com/jinpan/stuffdb/tableview"
	"github.com/jinpan/stuffdb/writestore"
)

type Table struct {
	name         string
	schema       *schema.Schema
	columns      []*column.Column
	n_entries    int
	insert_store *writestore.InsertStore
}

func NewTable(name string, schema *schema.Schema) *Table {
	// initialize write store
	filename := path.Join(
		"/var",
		"stuffdb",
		name,
	)

	mkdir_err := os.MkdirAll(filename, 0700)
	if mkdir_err != nil {
		panic(mkdir_err.Error())
	}
	is, is_err := writestore.NewInsertStore(name, schema)
	if is_err != nil {
		panic(is_err.Error())
	}

	columns := make([]*column.Column, schema.GetLen())
	for i := 0; i < schema.GetLen(); i++ {
		col := column.NewColumn(name, schema, i)
		columns[i] = col
	}

	return &Table{
		name:         name,
		schema:       schema,
		columns:      columns,
		n_entries:    0,
		insert_store: is,
	}
}

func (t *Table) Scan(columns ...int) tableview.TableView {
	ch := make(tableview.TableView, 1024)

	go func() {

		cols := make([]chan interface{}, len(columns))

		for idx, col_idx := range columns {
			cols[idx] = t.columns[col_idx].Scan()
		}

		for c0 := range cols[0] {
			row := make([]interface{}, len(columns))
			row[0] = c0

			for idx := 1; idx < len(columns); idx++ {
				row[idx] = <-cols[idx]
			}

			ch <- tableview.TableViewRow{row}
		}

		// consult the insert store
		insert_view := t.insert_store.ReadAll()
		for insert_store_row := range insert_view {

			row := make([]interface{}, len(columns))
			for idx, col_idx := range columns {
				row[idx] = insert_store_row.Data[col_idx]
			}
			ch <- tableview.TableViewRow{row}
		}

		close(ch)
	}()

	return ch
}

func (t *Table) Insert(row []interface{}) error {
	n_entries, insert_err := t.insert_store.Insert(row)
	if insert_err != nil {
		return insert_err
	}

	if n_entries == 1024 { // move the inserts from the insertstore to columns
		fmt.Println("MERGING")

		cache := make([][]interface{}, t.schema.GetLen())
		for i := 0; i < t.schema.GetLen(); i++ {
			cache[i] = make([]interface{}, 1024)
		}
		count := 0
		for row := range t.insert_store.ReadAll() {
			for i := 0; i < t.schema.GetLen(); i++ {
				cache[i][count] = row.Data[i]
			}
			count++
		}

		for i := 0; i < t.schema.GetLen(); i++ {
			ch := make(chan interface{}, 1024)
			go func(ch chan interface{}) {
				for _, datum := range cache[i] {
					ch <- datum
				}
				close(ch)
			}(ch)
			t.columns[i].Insert(ch, 1024)
		}

		t.insert_store.Clear()
	}

	return nil
}

func (t *Table) BulkInsert(rows chan []interface{}, size int) {
	// round off size
	col_store_size := size / 1024 * 1024
	// insert_store_size := size % 1024

	chans := make([]chan interface{}, t.schema.GetLen())
	for i := 0; i < t.schema.GetLen(); i++ {
		chans[i] = make(chan interface{})
	}
	go func() {
		for i := 0; i < col_store_size; i++ {
			row := <-rows
			for j := 0; j < t.schema.GetLen(); j++ {
				chans[j] <- row[j]
			}
		}
		for i := 0; i < t.schema.GetLen(); i++ {
			close(chans[i])
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < t.schema.GetLen(); i++ {
		wg.Add(1)
		go func(i int) {
			t.columns[i].Insert(chans[i], col_store_size)
			wg.Done()
		}(i)
	}
	wg.Wait()

	for row := range rows {
		n_entries, insert_err := t.insert_store.Insert(row)
		if insert_err != nil {
			panic(insert_err.Error())
		}
		if n_entries > 1024 {
			panic("Too many entries in the insert store")
		}
	}
}

func (t *Table) GetName() string {
	return t.name
}
