package table

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/jinpan/stuffdb/column"
	"github.com/jinpan/stuffdb/datatypes"
	"github.com/jinpan/stuffdb/schema"
	"github.com/jinpan/stuffdb/settings"
	"github.com/jinpan/stuffdb/tableview"
	"github.com/jinpan/stuffdb/writestore"
)

type Table struct {
	Name         string         `json:"name"`
	Schema       *schema.Schema `json:"schema"`
	N_entries    int            `json:"n_entries"`
	columns      []*column.Column
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
		Name:         name,
		Schema:       schema,
		columns:      columns,
		N_entries:    0,
		insert_store: is,
	}
}

func Load(name string) *Table {
	filename := path.Join(
		"/var",
		"stuffdb",
		name,
		"metadata",
	)

	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err.Error())
	}
	var t Table
	json.Unmarshal(bytes, &t)

	t.Schema.Types = make([]datatypes.DatumType, t.Schema.GetLen())
	for i := 0; i < t.Schema.GetLen(); i++ {
		t.Schema.Types[i] = datatypes.INT64_TYPE
	}

	t.columns = make([]*column.Column, t.Schema.GetLen())
	for i := 0; i < t.Schema.GetLen(); i++ {
		t.columns[i] = column.Load(name, t.Schema, i)
	}

	t.insert_store = writestore.Load(name, t.Schema, t.N_entries%1024)

	return &t
}

func (t *Table) Store() {
	filename := path.Join(
		"/var",
		"stuffdb",
		t.Name,
		"metadata",
	)

	bytes, err := json.Marshal(t)
	if err != nil {
		panic(err.Error())
	}
	err = ioutil.WriteFile(filename, bytes, 0600)
	if err != nil {
		panic(err.Error())
	}
}

func (t *Table) Scan(columns ...int) tableview.TableView {
	ch := make(tableview.TableView, settings.ChanSize)

	go func() {

		cols := make([]chan []interface{}, len(columns))

		for idx, col_idx := range columns {
			cols[idx] = t.columns[col_idx].Scan()
		}

		for cols0 := range cols[0] {
			rows := make(tableview.TableViewRows, len(cols0))

			for row_idx, col_val := range cols0 {
				rows[row_idx] = make(tableview.TableViewRow, len(columns))
				rows[row_idx][0] = col_val
			}

			for col_idx := 1; col_idx < len(columns); col_idx++ {
				col := <-cols[col_idx]
				for row_idx, col_val := range col {
					rows[row_idx][col_idx] = col_val
				}
			}

			ch <- rows
		}

		// consult the insert store
		insert_view := t.insert_store.ReadAll()
		for full_rows := range insert_view {
			rows := make(tableview.TableViewRows, len(full_rows))
			for row_idx, full_row := range full_rows {
				rows[row_idx] = make(tableview.TableViewRow, len(columns))
				for col_idx, full_col_idx := range columns {
					rows[row_idx][col_idx] = full_row[full_col_idx]
				}
			}
			ch <- rows
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

		cache := make([][]interface{}, t.Schema.GetLen())
		for i := 0; i < t.Schema.GetLen(); i++ {
			cache[i] = make([]interface{}, 1024)
		}
		count := 0
		for rows := range t.insert_store.ReadAll() {
			for _, row := range rows {
				for i := 0; i < t.Schema.GetLen(); i++ {
					cache[i][count] = row[i]
				}
				count++
			}
		}

		for i := 0; i < t.Schema.GetLen(); i++ {
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

	t.N_entries++
	t.Store()

	return nil
}

func (t *Table) BulkInsert(rows chan []interface{}, size int) {
	// round off size
	col_store_size := size / 1024 * 1024
	// insert_store_size := size % 1024

	chans := make([]chan interface{}, t.Schema.GetLen())
	for i := 0; i < t.Schema.GetLen(); i++ {
		chans[i] = make(chan interface{})
	}
	go func() {
		for i := 0; i < col_store_size; i++ {
			row := <-rows
			for j := 0; j < t.Schema.GetLen(); j++ {
				chans[j] <- row[j]
			}
		}
		for i := 0; i < t.Schema.GetLen(); i++ {
			close(chans[i])
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < t.Schema.GetLen(); i++ {
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

	t.Store()
}

func (t *Table) GetName() string {
	return t.Name
}
