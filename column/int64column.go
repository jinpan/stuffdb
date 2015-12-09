package column

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/jinpan/stuffdb/datatypes"
	"github.com/jinpan/stuffdb/settings"
)

/*
	This represents a physical column on disk.

	For a given table's column, there can be several physical columns
	representing multiple banks that may be
*/

type PhysicalInt64 struct {
	filename string
	data_len int
}

func NewPhysicalInt64(
	filename string,
	data <-chan interface{},
	size int,
) *PhysicalInt64 {

	f, create_err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if create_err != nil {
		panic(create_err.Error())
	}
	defer f.Close()

	i := 0
	for done := false; !done; {
		buf := new(bytes.Buffer)
		for j := 0; j < 1024; i, j = i+1, j+1 {
			datum, ok := <-data
			if ok {
				if err := binary.Write(buf, binary.LittleEndian, datum.(int64)); err != nil {
					panic(err.Error())
				}
			} else {
				done = true
				break
			}
		}
		_, write_err := f.Write(buf.Bytes())
		if write_err != nil {
			panic(write_err.Error())
		}
	}
	if i != size {
		panic("size mismatch")
	}

	return &PhysicalInt64{
		filename: filename,
		data_len: size,
	}
}

func LoadPhysicalInt64(filename string, size int) *PhysicalInt64 {
	return &PhysicalInt64{
		filename: filename,
		data_len: size,
	}
}

func (p *PhysicalInt64) Move(filename string) {
	err := os.Rename(p.filename, filename)
	if err != nil {
		panic(err.Error())
	}
	p.filename = filename
}

func (p *PhysicalInt64) Merge(o *PhysicalInt64, filename string) *PhysicalInt64 {

	data1 := p.ReadAll()
	data2 := o.ReadAll()

	ch := make(chan interface{})
	go func() {
		count := 0
		for datum := range data1 {
			ch <- datum
			count++
		}
		for datum := range data2 {
			ch <- datum
			count++
		}
		close(ch)
	}()

	return NewPhysicalInt64(filename, ch, p.data_len+o.data_len)
}

func (p *PhysicalInt64) Delete() {
	remove_err := os.Remove(p.filename)
	if remove_err != nil {
		panic(remove_err.Error())
	}
}

func (p *PhysicalInt64) GetSize() int {
	return p.data_len
}

func (p *PhysicalInt64) ReadOne(i int) (interface{}, error) {
	ch, err := p.Read(i, i+1)
	if err != nil {
		return nil, err
	}

	return <-ch, nil
}

func (p *PhysicalInt64) ReadAll() <-chan interface{} {
	ch, err := p.Read(0, p.data_len)
	if err != nil {
		panic(err.Error())
	}
	return ch
}

// exclusive
func (p *PhysicalInt64) Read(i, j int) (<-chan interface{}, error) {
	if j < i {
		return nil, fmt.Errorf("Second index must be at least as big as the first")
	}

	f, open_err := os.Open(p.filename) // open for reading
	if open_err != nil {
		panic(open_err.Error())
	}

	n_records := j - i
	datum_size := datatypes.INT64_TYPE.GetSize()

	buf := make([]byte, datum_size*settings.BatchSize)
	data := make([]int64, settings.BatchSize)

	ch := make(chan interface{}, 1024)

	go func() {
		cleanup := func() {
			close(ch)
			if close_err := f.Close(); close_err != nil {
				panic(close_err.Error())
			}
		}

		read_fun := func(amount_bytes, offset_bytes int) {
			buf_copy := buf[:amount_bytes]
			data_copy := data[:amount_bytes/datum_size]

			if _, read_err := f.ReadAt(buf_copy, int64(offset_bytes)); read_err != nil {
				cleanup()
				panic(read_err.Error())
			}

			if bin_read_err := binary.Read(
				bytes.NewReader(buf_copy),
				binary.LittleEndian,
				data_copy,
			); bin_read_err != nil {
				cleanup()
				panic(bin_read_err.Error())
			}

			for _, datum := range data_copy {
				ch <- datum
			}
		}

		var k int
		for k = 0; k < n_records/settings.BatchSize; k++ {
			amount_bytes := settings.BatchSize * datum_size
			offset_bytes := (i + k*settings.BatchSize) * datum_size

			read_fun(amount_bytes, offset_bytes)
		}
		if n_records%settings.BatchSize != 0 {
			amount_bytes := (n_records % settings.BatchSize) * datum_size
			offset_bytes := (i + k*settings.BatchSize) * datum_size

			read_fun(amount_bytes, offset_bytes)
		}
		cleanup()
	}()

	return ch, nil
}
