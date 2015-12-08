package column

import (
	"fmt"
	"os"
	"path"
	"testing"
)

const (
	n_records = 10000
)

func setup_int64(t *testing.T) (*PhysicalInt64, []interface{}) {
	err := os.MkdirAll("/var/stuffdb/test_table/c0", 0700)
	if err != nil {
		t.Error(err.Error())
	}

	data := make([]interface{}, 0, n_records)
	for i := int64(0); i < int64(n_records); i++ {
		data = append(data, i)
	}
	ch := make(chan interface{})
	go func() {
		for i := 0; i < n_records; i++ {
			ch <- data[i]
		}
		close(ch)
	}()

	physical := NewPhysicalInt64(
		fmt.Sprintf("/var/stuffdb/test_table/c0/%d", n_records),
		ch,
		n_records,
	)

	if physical.data_len != n_records {
		t.Errorf("Did not set physical size correctly")
	}

	return physical, data
}

func cleanup(t *testing.T) {
	r := recover()

	testpath := path.Join(
		"/var",
		"stuffdb",
		"test_table",
	)
	remove_err := os.RemoveAll(testpath)
	if remove_err != nil {
		t.Errorf(remove_err.Error())
	}

	if r != nil {
		panic(r)
	}
}

func TestCreateInt64(t *testing.T) {
	setup_int64(t)
	defer cleanup(t)
}

func TestReadAll(t *testing.T) {
	physical, data := setup_int64(t)
	defer cleanup(t)

	// test read batch
	actual := physical.ReadAll()
	if actual == nil {
		t.Errorf("")
	}

	count := 0
	for datum := range actual {
		if datum.(int64) != data[count] {
			t.Errorf("Expected %d, got %d", data[count], datum.(int64))
		}
		count++
	}
	if count != n_records {
		t.Errorf("Expected length %d result, got %d", n_records, count)
	}
}

func TestReadBatch(t *testing.T) {
	physical, data := setup_int64(t)
	defer cleanup(t)

	// test read batch
	actual, err := physical.Read(0, n_records)
	if err != nil {
		t.Errorf("Expected no error in batch read, got %s", err.Error())
	}
	if actual == nil {
		t.Errorf("")
	}

	count := 0
	for datum := range actual {
		if datum.(int64) != data[count] {
			t.Errorf("Expected %d, got %d", data[count], datum.(int64))
		}
		count++
	}
	if count != n_records {
		t.Errorf("Expected length %d result, got %d", n_records, count)
	}
}

func TestReadIndividual(t *testing.T) {
	physical, data := setup_int64(t)
	defer cleanup(t)

	// test read one at a time (skip over 6/7 for perf)
	for i := 0; i < n_records; i += 7 {
		datum, read_err := physical.ReadOne(i)
		if read_err != nil {
			t.Errorf(read_err.Error())
		}

		if datum.(int64) != data[i] {
			t.Errorf("Expected %d, got %d", i, datum.(int64))
		}
	}
}

func TestMoveInt64(t *testing.T) {
	physical, data := setup_int64(t)
	defer cleanup(t)

	// test read batch
	actual, err := physical.Read(0, n_records)
	if err != nil {
		t.Errorf("Expected no error in batch read, got %s", err.Error())
	}
	count := 0
	for datum := range actual {
		if datum.(int64) != data[count] {
			t.Errorf("Expected %d, got %d", data[count], datum.(int64))
		}
		count++
	}
	if count != n_records {
		t.Errorf("Expected length %d result, got %d", n_records, count)
	}
}

func TestMergeInt64(t *testing.T) {
	physical1, data1 := setup_int64(t)
	defer cleanup(t)

	data2 := make([]interface{}, 0, n_records)
	for i := int64(0); i < int64(n_records); i++ {
		data2 = append(data2, int64(n_records)-i)
	}

	ch := make(chan interface{})
	go func() {
		for _, datum := range data2 {
			ch <- datum
		}
		close(ch)
	}()

	physical2 := NewPhysicalInt64(
		fmt.Sprintf("/var/stuffdb/test_table/c0/%d_b", n_records),
		ch,
		n_records,
	)

	physical3 := physical1.Merge(
		physical2,
		fmt.Sprintf("/var/stuffdb/test_table/c0/%d_c", n_records),
	)

	// test read batch
	expected := append(data1, data2...)
	actual, err := physical3.Read(0, 2*n_records)
	if err != nil {
		t.Errorf("Expected no error in batch read, got %s", err.Error())
	}
	count := 0
	for datum := range actual {
		if datum.(int64) != expected[count] {
			t.Errorf("Expected %d, got %d", expected[count], datum.(int64))
		}
		count++
	}
	if count != 2*n_records {
		t.Errorf("Expected length %d result, got %d", 2*n_records, count)
	}
}
