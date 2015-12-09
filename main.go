package main

import (
	"fmt"
	"time"

	"github.com/jinpan/stuffdb/table"
	"github.com/jinpan/stuffdb/tableview"
)

func filter_census(t *table.Table) {
	cond := func(x interface{}) bool {
		return x.(int64) == 10914
	}

	tv := tableview.Filter(t.Scan(0, 1), 0, cond)

	fmt.Println("START", time.Now())
	for row := range tv {
		fmt.Println(row)
	}
	fmt.Println("END", time.Now())
}

func main() {
	t := import_census()
	filter_census(t)
}
