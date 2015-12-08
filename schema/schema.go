package schema

import (
	"fmt"
	"strings"

	"github.com/jinpan/stuffdb/datatypes"
)

/*
	Currently, we require schemas to be immutable
*/
type Schema struct {
	names          []string // names must be unique and lower case
	types          []datatypes.DatumType
	row_size_bytes int // naive size of the row if we store it varbatim
}

func NewSchema(names []string, types []datatypes.DatumType) (*Schema, error) {
	// check size mismatch
	if len(names) != len(types) {
		return nil, fmt.Errorf("Size mismatch: |names| = %d, |types| = %d",
			len(names), len(types))
	}

	// check that names are lower cased
	for _, name := range names {
		if name != strings.ToLower(name) {
			return nil, fmt.Errorf("All names should be lower cased")
		}
	}

	// check for duplicate names
	name_set := make(map[string]bool)
	for _, name := range names {
		name_set[name] = true
	}
	if len(name_set) < len(names) {
		return nil, fmt.Errorf("All names should be unique")
	}

	row_size_bytes := 0
	for _, data_type := range types {
		row_size_bytes += data_type.GetSize()
	}
	schema := &Schema{
		names:          make([]string, len(names)),
		types:          make([]datatypes.DatumType, len(types)),
		row_size_bytes: row_size_bytes,
	}
	copy(schema.names, names)
	copy(schema.types, types)

	return schema, nil
}

func (s *Schema) GetRowSizeBytes() int {
	return s.row_size_bytes
}

func (s *Schema) GetName(i int) string {
	return s.names[i]
}

func (s *Schema) GetType(i int) datatypes.DatumType {
	return s.types[i]
}

func (s *Schema) GetLen() int {
	return len(s.names)
}
