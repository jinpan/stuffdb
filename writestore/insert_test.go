package writestore

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
		t.Errorf(mkdir_err.Error())
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

func TestCreate(t *testing.T) {
	setup(t)
	defer cleanup(t)

	schema := makeSchema(t)
	insert_store, create_err := NewInsertStore("test_table", schema)
	if create_err != nil {
		t.Errorf(create_err.Error())
	}
	if insert_store == nil {
		t.Errorf("Insert store should not be null")
	}
}

func TestWriteRead(t *testing.T) {
	setup(t)
	defer cleanup(t)

	schema := makeSchema(t)
	insert_store, create_err := NewInsertStore("test_table", schema)
	if create_err != nil {
		t.Errorf(create_err.Error())
	}
	if insert_store == nil {
		t.Errorf("Insert store should not be null")
	}

	n_records := 1000
	for i := 0; i < n_records; i++ {
		data := []interface{}{
			int64(i),
			int64(2 * i),
		}
		n_entries, insert_err := insert_store.Insert(data)
		if insert_err != nil {
			t.Errorf(insert_err.Error())
		}
		if n_entries != i+1 {
			t.Errorf("Incorrect number of entries in the insert store")
		}
	}

	count := 0
	ch, read_err := insert_store.Read(0, n_records-1)
	if read_err != nil {
		t.Errorf(read_err.Error())
	}
	for record := range ch {
		if record.Data[0].(int64) != int64(count) {
			t.Errorf("Expected %d, got %d", count, record.Data[0].(int64))
		}
		if record.Data[1].(int64) != int64(2*count) {
			t.Errorf("Expected %d, got %d", 2*count, record.Data[0].(int64))
		}
		if len(record.Data) != schema.GetLen() {
			t.Errorf("Expected a length of %d, got %d", schema.GetLen, len(record.Data))
		}
		count++
	}

}
