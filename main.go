package main

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/jinpan/stuffdb/table"
	"github.com/jinpan/stuffdb/tableview"
)

func filter_census(t *table.Table, c int) time.Duration {
	cond := func(x interface{}) bool {
		return x.(int64) == 0
	}

	cols := make([]int, c)
	for i := 0; i < c; i++ {
		cols[i] = i
	}
	tv := tableview.Filter(t.Scan(cols...), 0, cond)

	start_time := time.Now()
	for rows := range tv {
		for _, row := range rows {
			fmt.Println(row)
		}
	}
	end_time := time.Now()
	return end_time.Sub(start_time)
}

func main() {
	debug.SetGCPercent(3200)
	t := table.Load("test_census")

	for i := 1; i < 256; i *= 2 {
		var min_duration time.Duration
		for j := 0; j < 3; j++ {
			runtime.GC()
			d := filter_census(t, i)
			runtime.GC()
			if min_duration == 0 {
				min_duration = d
			} else if d < min_duration {
				min_duration = d
			}
		}
		fmt.Println(i, min_duration)
	}
}
