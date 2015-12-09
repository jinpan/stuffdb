package column

import (
	"container/list"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/jinpan/stuffdb/datatypes"
	"github.com/jinpan/stuffdb/schema"
)

type Column struct {
	tablename string
	base_dir  string
	schema    *schema.Schema
	rank      int
	primary   *list.List // list of columns ordered by the primary key
}

func NewColumn(tablename string, schema *schema.Schema, rank int) *Column {
	base_dir := filepath.Join(
		"/var",
		"stuffdb",
		tablename,
		fmt.Sprintf("c%d", rank),
	)
	mkdir_err := os.MkdirAll(base_dir, 0700)
	if mkdir_err != nil {
		panic(mkdir_err.Error())
	}
	return &Column{
		tablename: tablename,
		base_dir:  base_dir,
		schema:    schema,
		rank:      rank,
		primary:   list.New(),
	}
}

func Load(tablename string, s *schema.Schema, rank int) *Column {
	base_dir := filepath.Join(
		"/var",
		"stuffdb",
		tablename,
		fmt.Sprintf("c%d", rank),
	)
	c := Column{
		tablename: tablename,
		base_dir:  base_dir,
		schema:    s,
		rank:      rank,
		primary:   list.New(),
	}

	files, err := ioutil.ReadDir(base_dir)
	if err != nil {
		panic(err.Error())
	}

	file_map := make(map[int]string)
	for _, fi := range files {
		fname := fi.Name()
		iname, err := strconv.Atoi(fname)
		if err != nil {
			panic(err.Error())
		}
		file_map[iname] = fname
	}
	sizes := make([]int, len(file_map))
	for iname, _ := range file_map {
		sizes = append(sizes, iname)
	}
	sort.Ints(sizes)
	for _, size := range sizes {
		col := LoadPhysicalInt64(filepath.Join(base_dir, file_map[size]), size)
		c.primary.PushBack(col)
	}

	return &c
}

func (c *Column) Scan() chan interface{} {
	ch := make(chan interface{})

	go func() {
		defer close(ch)

		for node := c.primary.Front(); node != nil; node = node.Next() {
			physical := node.Value.(Physical)
			pch := physical.ReadAll()
			for datum := range pch {
				ch <- datum
			}
		}
	}()

	return ch
}

func (c *Column) GetDatum(i int) (interface{}, error) {
	for node := c.primary.Front(); node != nil; node = node.Next() {
		physical := node.Value.(Physical)
		if i < physical.GetSize() {
			return physical.ReadOne(i)
			break
		} else {
			i -= physical.GetSize()
		}
	}

	return nil, fmt.Errorf("out of bounds")
}

func (c *Column) Insert(data <-chan interface{}, size int) {
	old_size := 0
	for node := c.primary.Front(); node != nil; node = node.Next() {
		old_size += node.Value.(Physical).GetSize()
	}
	new_size := old_size + size

	ch := c.Scan()

	new_nodes := list.New()

	for col_size := 0; new_size > 0; new_size -= col_size {
		col_size = 1
		for new_size_tmp := new_size; new_size_tmp > 0; new_size_tmp >>= 1 {
			col_size <<= 1
		}
		col_size >>= 1
		col_ch := make(chan interface{})

		go func() {
			var old_copy int
			if col_size > old_size {
				old_copy = old_size
			} else {
				old_copy = col_size
			}

			for i := 0; i < old_copy; i++ {
				col_ch <- <-ch
			}
			old_size -= old_copy

			for i := 0; i < col_size-old_copy; i++ {
				col_ch <- <-data
			}

			close(col_ch)
		}()
		filename := filepath.Join(c.base_dir, fmt.Sprintf("%d_tmp", col_size))
		physical := NewPhysicalInt64(filename, col_ch, col_size)
		new_nodes.PushBack(physical)
	}

	for node := c.primary.Front(); node != nil; node = node.Next() {
		node.Value.(Physical).Delete()
	}

	for node := new_nodes.Front(); node != nil; node = node.Next() {
		size := node.Value.(Physical).GetSize()
		filename := filepath.Join(c.base_dir, fmt.Sprintf("%d", size))
		node.Value.(Physical).Move(filename)
	}

	c.primary = new_nodes
}

func (c *Column) insert(data <-chan interface{}, size int) (Physical, error) {

	back := c.primary.Back()
	// check it conflicts with the last node
	if back == nil || back.Value.(Physical).GetSize() != size { // no conflict
		switch c.schema.GetType(c.rank) {
		case datatypes.INT64_TYPE:
			filename := filepath.Join(c.base_dir, fmt.Sprintf("%d", size))
			p_col := NewPhysicalInt64(filename, data, size)
			c.primary.PushBack(p_col)
			return p_col, nil
		default:
			return nil, fmt.Errorf("Invalid schema type")
		}
	} else { // merge conflict
		p_col := back.Value.(Physical)
		old_data := p_col.ReadAll()

		new_size := p_col.GetSize() + size
		new_data := make(chan interface{})
		go func() {
			for datum := range old_data {
				new_data <- datum
			}
			for datum := range data {
				new_data <- datum
			}
			close(new_data)
		}()

		c.primary.Remove(back)
		new_p_col, insert_err := c.insert(new_data, new_size)
		if insert_err != nil {
			return nil, insert_err
		}

		p_col.Delete()

		return new_p_col, nil
	}
}
