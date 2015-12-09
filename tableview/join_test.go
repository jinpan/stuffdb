package tableview

import "testing"

func TestFilter(t *testing.T) {
	filter_func := func(x interface{}) bool {
		return x.(int64)%3 == 0
	}

	n_records := 10000

	input := make(TableView)
	output := Filter(input, 1, filter_func)

	go func() {
		defer close(input)

		for i := int64(0); i < int64(n_records); i++ {
			input <- TableViewRows{
				TableViewRow{0, int64(i)},
			}
		}
	}()

	output_count := 0
	for rows := range output {
		for _, row := range rows {
			if !filter_func(row[1]) {
				t.Errorf("%s should have failed", row[1])
			}
			output_count++
		}
	}

	if output_count != n_records/3+1 {
		t.Errorf("Expected %d outputs, got %d", n_records/3+1, output_count)
	}
}
