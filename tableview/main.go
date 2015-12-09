package tableview

import "github.com/jinpan/stuffdb/settings"

type TableViewRow []interface{}
type TableViewRows []TableViewRow

type TableView (chan TableViewRows)

type ColumnView (chan interface{})

func Filter(tv TableView, col_idx int, cond func(interface{}) bool) TableView {
	output := make(TableView, settings.ChanSize)

	go func() {
		defer close(output)

		for rows := range tv {
			result := make(TableViewRows, 0, len(rows))
			for _, row := range rows {
				if cond(row[col_idx]) {
					result = append(result, row)
				}
			}
			output <- result
		}
	}()

	return output
}

// General equijoin on unsorted, assume everything fits in memory for now
func EquiJoin(tv1, tv2 TableView, col_idx1, col_idx2 int) TableView {
	output := make(TableView, settings.ChanSize)

	go func() {
		defer close(output)

		tv1_map := make(map[interface{}][]TableViewRow)

		for rows := range tv1 {
			for _, row := range rows {
				tv1_map[row[col_idx1]] = append(tv1_map[row[col_idx1]], row)
			}
		}

		for rows2 := range tv2 {
			for _, row2 := range rows2 {
				if rows1 := tv1_map[row2[col_idx2]]; rows1 != nil {
					for _, row1 := range rows1 {
						output <- TableViewRows{append(row1, row2...)}
					}
				}
			}
		}
	}()

	return output
}
