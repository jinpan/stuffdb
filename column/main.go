package column

var (
	_ Physical = &PhysicalInt64{}
)

type Physical interface {
	Delete()

	GetSize() int
	ReadOne(int) (interface{}, error)
	ReadAll() <-chan []interface{}
	Read(int, int) (<-chan []interface{}, error)
	Move(string)
}
