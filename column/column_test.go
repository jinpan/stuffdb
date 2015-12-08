package column

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/jinpan/stuffdb/datatypes"
	"github.com/jinpan/stuffdb/schema"
)

func column_setup(t *testing.T) {
	err := os.MkdirAll("/var/stuffdb/test_table/c0", 0700)
	if err != nil {
		t.Error(err.Error())
	}
}

func column_cleanup(t *testing.T) {
	testpath := path.Join(
		"/var",
		"stuffdb",
		"test_table",
	)
	remove_err := os.RemoveAll(testpath)
	if remove_err != nil {
		t.Errorf(remove_err.Error())
	}
}

func TestInsert(t *testing.T) {
	column_setup(t)
	defer column_cleanup(t)

	names1 := []string{
		"a",
		"b",
	}
	types1 := []datatypes.DatumType{
		datatypes.INT64_TYPE,
		datatypes.INT64_TYPE,
	}
	schema, err := schema.NewSchema(names1, types1)
	if err != nil {
		t.Errorf(err.Error())
	}

	col := NewColumn("test_table", schema, 0)
	if col == nil {
		t.Errorf("Unable to create new column")
	}

	n_records := 4096

	data := make([]interface{}, n_records)

	for i := int64(0); i < int64(n_records); i++ {
		data[i] = i
	}

	// test the first insertion (no merge)
	ch1 := make(chan interface{})
	go func() {
		for i := int64(0); i < 1024; i++ {
			ch1 <- i
		}
		close(ch1)
	}()
	col.Insert(ch1, 1024)

	if col.primary.Len() != 1 {
		t.Errorf("Incorrect number of physical columns")
	}

	for i := 0; i < 1024; i++ {
		datum, err := col.GetDatum(i)
		if err != nil {
			t.Errorf(err.Error())
		}
		if datum.(int64) != data[i] {
			t.Errorf("Read back the wrong value")
		}
	}

	files, err := ioutil.ReadDir(col.base_dir)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(files) != 1 {
		t.Errorf("Expected 1 files, got %d", len(files))
	}

	// test the second insertion (1 merge)
	ch2 := make(chan interface{})
	go func() {
		for i := int64(1024); i < 2048; i++ {
			ch2 <- i
		}
		close(ch2)
	}()
	col.Insert(ch2, 1024)

	if col.primary.Len() != 1 {
		t.Errorf("Expected %d physical columns, got %d", 1, col.primary.Len())
	}

	for i := 0; i < 2048; i++ {
		datum, err := col.GetDatum(i)
		if err != nil {
			t.Errorf(err.Error())
		}
		if datum.(int64) != data[i] {
			t.Errorf("Read back the wrong value")
		}
	}

	files, err = ioutil.ReadDir(col.base_dir)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(files) != 1 {
		t.Errorf("Expected %d files, got %d", 1, len(files))
	}

	// test the third insertion (no merge)
	ch3 := make(chan interface{})
	go func() {
		for i := int64(2048); i < 3072; i++ {
			ch3 <- data[i]
		}
		close(ch3)
	}()
	col.Insert(ch3, 1024)

	if col.primary.Len() != 2 {
		t.Errorf("Expected %d physical columns, got %d", 2, col.primary.Len())
		fmt.Println(col.primary.Front().Value.(Physical).GetSize())
	}

	for i := 0; i < 3072; i++ {
		datum, err := col.GetDatum(i)
		if err != nil {
			fmt.Println("XXX", col.primary.Front().Value.(Physical).GetSize())
			fmt.Println("XXX", col.primary.Front().Next().Value.(Physical).GetSize())
			t.Errorf(err.Error())
		}
		if datum.(int64) != data[i] {
			t.Errorf("Read back the wrong value")
		}
	}

	files, err = ioutil.ReadDir(col.base_dir)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(files) != 2 {
		t.Errorf("Expected %d files, got %d", 2, len(files))
	}

	// test the fourth insertion (2 merge)
	ch4 := make(chan interface{})
	go func() {
		for i := int64(3072); i < 4096; i++ {
			ch4 <- data[i]
		}
		close(ch4)
	}()
	col.Insert(ch4, 1024)

	if col.primary.Len() != 1 {
		t.Errorf("Expected %d physical columns, got %d", 1, col.primary.Len())
	}

	for i := 0; i < 4096; i++ {
		datum, err := col.GetDatum(i)
		if err != nil {
			t.Errorf(err.Error())
		}
		if datum.(int64) != data[i] {
			t.Errorf("Read back the wrong value")
		}
	}

	files, err = ioutil.ReadDir(col.base_dir)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(files) != 1 {
		t.Errorf("Expected %d files, got %d", 1, len(files))
	}
}
