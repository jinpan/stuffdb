package table

import (
	"os"
	"path"
	"testing"

	"github.com/jinpan/stuffdb/datatypes"
	"github.com/jinpan/stuffdb/schema"
)

const (
	TEST_TABLE_NAME = "test_table"
)

var (
	TEST_PATH = path.Join(
		"/var",
		"stuffdb",
		TEST_TABLE_NAME,
	)
)

func setup(t *testing.T) {
	mkdir_err := os.MkdirAll(TEST_PATH, 0700)
	if mkdir_err != nil {
		t.Error(mkdir_err.Error())
	}
}

func cleanup(t *testing.T) {
	remove_err := os.RemoveAll(TEST_PATH)
	if remove_err != nil {
		t.Errorf(remove_err.Error())
	}
}

func makeSchema(t *testing.T) *schema.Schema {
	names1 := []string{
		"a",
		"b",
	}
	types1 := []datatypes.DatumType{
		datatypes.INT64_TYPE,
		datatypes.INT64_TYPE,
	}
	schema, err := schema.NewSchema(names1, types1)
	if schema == nil {
		t.Error("expected schema to exist")
	}
	if err != nil {
		t.Errorf("expected no error here")
	}
	if schema.GetRowSizeBytes() != 16 {
		t.Error("Expected row size to be 16 bytes, got %d", schema.GetRowSizeBytes())
	}
	return schema
}

func TestCreateTable(t *testing.T) {
	setup(t)
	defer cleanup(t)

	table_name := "test_table"
	s := makeSchema(t)

	table := NewTable(table_name, s)
	if table == nil {
		t.Errorf("Unable to create new table")
	}

	n_tests := 5000

	for i := 0; i < n_tests; i++ {
		row := []interface{}{int64(i), int64(2 * i)}
		insert_err := table.Insert(row)
		if insert_err != nil {
			t.Errorf(insert_err.Error())
		}

		tv := table.Scan(0, 1)

		row_count := 0
		for tr := range tv {
			if len(tr.Data) != 2 {
				t.Errorf("Expected data length to be 2, got %d", len(tr.Data))
			}
			if tr.Data[0] != int64(row_count) {
				t.Errorf("Expected data[0] to be %d, got %d", row_count, tr.Data[0])
			}
			if tr.Data[1] != int64(2*row_count) {
				t.Errorf("Expected data[0] to be %d, got %d", 2*row_count, tr.Data[1])
			}

			row_count++
		}

		if row_count != i+1 {
			t.Errorf("Expected %d rows, got %d", i+1, row_count)
		}
	}
}

func TestLoad(t *testing.T) {
	setup(t)
	defer cleanup(t)

	table_name := "test_table"
	s := makeSchema(t)

	table := NewTable(table_name, s)
	if table == nil {
		t.Errorf("Unable to create new table")
	}

	n_tests := 2000

	for i := 0; i < n_tests; i++ {
		row := []interface{}{int64(i), int64(2 * i)}
		insert_err := table.Insert(row)
		if insert_err != nil {
			t.Errorf(insert_err.Error())
		}

		tv := table.Scan(0, 1)

		row_count := 0
		for tr := range tv {
			if len(tr.Data) != 2 {
				t.Errorf("Expected data length to be 2, got %d", len(tr.Data))
			}
			if tr.Data[0] != int64(row_count) {
				t.Errorf("Expected data[0] to be %d, got %d", row_count, tr.Data[0])
			}
			if tr.Data[1] != int64(2*row_count) {
				t.Errorf("Expected data[0] to be %d, got %d", 2*row_count, tr.Data[1])
			}

			row_count++
		}

		if row_count != i+1 {
			t.Errorf("Expected %d rows, got %d", i+1, row_count)
		}

		table2 := Load("test_table")
		tv = table2.Scan(0, 1)

		row_count = 0
		for tr := range tv {
			if len(tr.Data) != 2 {
				t.Errorf("Expected data length to be 2, got %d", len(tr.Data))
			}
			if tr.Data[0] != int64(row_count) {
				t.Errorf("Expected data[0] to be %d, got %d", row_count, tr.Data[0])
			}
			if tr.Data[1] != int64(2*row_count) {
				t.Errorf("Expected data[0] to be %d, got %d", 2*row_count, tr.Data[1])
			}

			row_count++
		}

		if row_count != i+1 {
			t.Errorf("Expected %d rows, got %d", i+1, row_count)
		}
	}
}
