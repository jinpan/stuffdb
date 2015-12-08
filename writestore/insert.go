package writestore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path"

	"github.com/jinpan/stuffdb/datatypes"
	"github.com/jinpan/stuffdb/schema"
	"github.com/jinpan/stuffdb/tableview"
)

const (
	MAX_SIZE = 1000
)

type InsertStore struct {
	tablename string
	schema    *schema.Schema
	n_entries int
	filename  string
}

func NewInsertStore(tablename string, schema *schema.Schema) (*InsertStore, error) {
	// initialize insert store
	filename := path.Join(
		"/var",
		"stuffdb",
		tablename,
		"insert_buffer",
	)

	f, create_err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL, 0700)
	if create_err != nil {
		return nil, create_err
	}
	close_err := f.Close()
	if close_err != nil {
		return nil, close_err
	}

	return &InsertStore{
		tablename: tablename,
		schema:    schema,
		n_entries: 0,
		filename:  filename,
	}, nil
}

func (w *InsertStore) Clear() error {
	rm_err := os.Remove(w.filename)
	if rm_err != nil {
		return rm_err
	}
	f, create_err := os.OpenFile(w.filename, os.O_CREATE|os.O_EXCL, 0700)
	if create_err != nil {
		return create_err
	}
	close_err := f.Close()
	if close_err != nil {
		return close_err
	}

	w.n_entries = 0
	return nil
}

func (w *InsertStore) Insert(row []interface{}) (int, error) {
	f, open_err := os.OpenFile(w.filename, os.O_RDWR|os.O_APPEND, 0700)
	if open_err != nil {
		return -1, open_err
	}
	defer f.Close()

	return w.n_entries, w.insert(f, row)
}

func (w *InsertStore) insert(f *os.File, row []interface{}) error {
	if len(row) != w.schema.GetLen() {
		return fmt.Errorf("Size mismatch between schema size and data size")
	}
	buf := new(bytes.Buffer)
	for i, datum := range row {
		switch w.schema.GetType(i) {
		case datatypes.INT64_TYPE:
			if err := binary.Write(buf, binary.LittleEndian, datum.(int64)); err != nil {
				return err
			}
		case datatypes.FLOAT64_TYPE:
			if err := binary.Write(buf, binary.LittleEndian, datum.(float64)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("Invalid data type")
		}
	}
	n_bytes, write_err := f.Write(buf.Bytes())
	if write_err != nil {
		return write_err
	}
	if n_bytes != w.schema.GetRowSizeBytes() {
		return fmt.Errorf("Expected to write %d bytes, wrote %d bytes",
			w.schema.GetRowSizeBytes(), n_bytes)
	}
	w.n_entries++

	return nil
}

func (w *InsertStore) ReadAll() tableview.TableView {
	tv, read_err := w.Read(0, w.n_entries)
	if read_err != nil {
		panic(read_err.Error())
	}

	return tv
}

func (w *InsertStore) Read(i, j int) (tableview.TableView, error) {
	if i < 0 || i > w.n_entries {
		return nil, fmt.Errorf("Invalid start position")
	}
	if j < 0 || j > w.n_entries {
		return nil, fmt.Errorf("Invalid end position")
	}
	if j < i {
		return nil, fmt.Errorf("End position (%d) must be after start position (%d)", j, i)
	}

	f, open_err := os.OpenFile(w.filename, os.O_RDONLY, 0400)
	if open_err != nil {
		panic(open_err.Error())
	}

	ch := make(tableview.TableView)
	go w.read(f, i, j, ch) // takes care of closing the file when done
	return ch, nil
}

func (w *InsertStore) read(f *os.File, i, j int, ch tableview.TableView) {
	defer func() {
		close(ch)
		close_err := f.Close()
		if close_err != nil {
			panic(close_err.Error())
		}
	}()

	buf := make([]byte, w.schema.GetRowSizeBytes())

	for k := i; k < j; k++ {
		_, file_read_err := f.ReadAt(buf, int64(k*w.schema.GetRowSizeBytes()))
		if file_read_err != nil {
			panic(file_read_err)
		}
		reader := bytes.NewReader(buf)

		data := make([]interface{}, w.schema.GetLen())
		for l := 0; l < len(data); l++ {
			switch w.schema.GetType(l) {
			case datatypes.INT64_TYPE:
				var datum int64
				bin_read_err := binary.Read(reader, binary.LittleEndian, &datum)
				if bin_read_err != nil {
					panic(bin_read_err.Error())
				}
				data[l] = datum
			default:
				panic("Invalid data type")
			}
		}
		ch <- tableview.TableViewRow{data}
	}
}
