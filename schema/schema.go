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
	Names          []string              `json:"names"`
	Types          []datatypes.DatumType `json:"types"`
	Row_size_bytes int                   `json:"row_size_bytes"`
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
		Names:          make([]string, len(names)),
		Types:          make([]datatypes.DatumType, len(types)),
		Row_size_bytes: row_size_bytes,
	}
	copy(schema.Names, names)
	copy(schema.Types, types)

	return schema, nil
}

func (s *Schema) GetRowSizeBytes() int {
	return s.Row_size_bytes
}

func (s *Schema) GetName(i int) string {
	return s.Names[i]
}

func (s *Schema) GetType(i int) datatypes.DatumType {
	return s.Types[i]
}

func (s *Schema) GetLen() int {
	return len(s.Names)
}
