package tableview

type TableViewRow struct {
	Data []interface{}
}

type TableView (chan TableViewRow)

type ColumnView (chan interface{})

func Filter(tv TableView, col_idx int, cond func(interface{}) bool) TableView {
	output := make(chan TableViewRow)

	go func() {
		defer close(output)

		for row := range tv {
			if cond(row.Data[col_idx]) {
				output <- row
			}
		}
	}()

	return output
}

// General equijoin on unsorted, assume everything fits in memory for now
func EquiJoin(tv1, tv2 TableView, col_idx1, col_idx2 int) TableView {
	output := make(chan TableViewRow)

	go func() {
		defer close(output)

		tv1_map := make(map[interface{}][]TableViewRow)

		for row := range tv1 {
			tv1_map[row.Data[col_idx1]] = append(tv1_map[row.Data[col_idx1]], row)
		}

		for row2 := range tv2 {
			if rows_1 := tv1_map[row2.Data[col_idx2]]; rows_1 != nil {
				for _, row1 := range rows_1 {
					data := append(row1.Data, row2.Data...)
					output <- TableViewRow{data}
				}
			}
		}
	}()

	return output
}
