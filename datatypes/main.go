package datatypes

var (
	_ Datum = &Int64Datum{}
	_ Datum = &Float64Datum{}
)

type DatumType int

const (
	INT64_TYPE DatumType = iota
	FLOAT64_TYPE
)

type Datum interface {
	GetType() DatumType
	GetData() interface{}
}

// Get the size of a datatype in bytes
func (d DatumType) GetSize() int {
	switch d {
	case INT64_TYPE:
		return 8
	case FLOAT64_TYPE:
		return 8
	default:
		panic("Invalid data type")
	}

	return 0
}
