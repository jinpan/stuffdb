package datatypes

import "strconv"

type Int64Datum struct {
	datum int64
}

func NewInt64Datum(datum int64) *Int64Datum {
	return &Int64Datum{
		datum: datum,
	}
}

func NewInt64DatumFromString(input string) (*Int64Datum, error) {
	i, err := strconv.ParseInt(input, 10, 64)
	if err != nil {
		return nil, err
	}

	datum := &Int64Datum{
		datum: i,
	}
	return datum, nil
}

func (i Int64Datum) GetType() DatumType {
	return INT64_TYPE
}

func (i Int64Datum) GetData() interface{} {
	return i.datum
}
