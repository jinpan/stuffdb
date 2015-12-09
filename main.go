package main

import (
	"flag"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/jinpan/stuffdb/table"
	"github.com/jinpan/stuffdb/tableview"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func filter_census(t *table.Table) time.Duration {
	cond := func(x interface{}) bool {
		return x.(int64) == 10914
	}

	start_time := time.Now()
	tv := tableview.Filter(t.Scan(0, 1), 0, cond)
	for rows := range tv {
		for _, row := range rows {
			fmt.Println(row)
		}
	}
	end_time := time.Now()
	return end_time.Sub(start_time)
}

func main() {
	/*
		flag.Parse()
		if *cpuprofile != "" {
			f, err := os.Create(*cpuprofile)
			if err != nil {
				log.Fatal(err)
			}
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}
	*/
	debug.SetGCPercent(1000)
	t := table.Load("test_census")

	var total_time time.Duration
	for i := 0; i < 10; i++ {
		total_time += filter_census(t)
	}
	fmt.Println("DURATION", total_time/10)
}
