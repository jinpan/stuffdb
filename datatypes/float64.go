package datatypes

import "strconv"

type Float64Datum struct {
	datum float64
}

func NewFloat64Datum(datum float64) *Float64Datum {
	return &Float64Datum{
		datum: datum,
	}
}

func NewFloat64DatumFromString(input string) (*Float64Datum, error) {
	f, err := strconv.ParseFloat(input, 64)
	if err != nil {
		return nil, err
	}

	datum := &Float64Datum{
		datum: f,
	}
	return datum, nil
}

func (f Float64Datum) GetType() DatumType {
	return FLOAT64_TYPE
}

func (f Float64Datum) GetData() interface{} {
	return f.datum
}
