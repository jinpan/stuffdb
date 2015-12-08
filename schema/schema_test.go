package schema

import (
	"testing"

	"github.com/jinpan/stuffdb/datatypes"
)

func TestSizeMismatch(t *testing.T) {
	names1 := []string{
		"a",
		"b",
	}
	types1 := []datatypes.DatumType{
		datatypes.INT64_TYPE,
	}
	schema, err := NewSchema(names1, types1)
	if schema != nil {
		t.Error("expected schema to not exist")
	}
	if err == nil {
		t.Errorf("expected error here")
	}
}

func TestNames(t *testing.T) {
	names1 := []string{
		"A",
		"b",
	}
	types1 := []datatypes.DatumType{
		datatypes.INT64_TYPE,
		datatypes.INT64_TYPE,
	}

	schema, err := NewSchema(names1, types1)
	if schema != nil {
		t.Error("expected schema to not exist: capital letters")
	}
	if err == nil {
		t.Errorf("expected error here")
	}

	names2 := []string{
		"a",
		"a",
	}
	schema, err = NewSchema(names2, types1)
	if schema != nil {
		t.Error("expected schema to not exist: duplicate name")
	}
	if err == nil {
		t.Errorf("expected error here")
	}
}

func TestSuccess(t *testing.T) {
	names1 := []string{
		"a",
		"b",
	}
	types1 := []datatypes.DatumType{
		datatypes.INT64_TYPE,
		datatypes.INT64_TYPE,
	}
	schema, err := NewSchema(names1, types1)
	if schema == nil {
		t.Error("expected schema to exist")
	}
	if err != nil {
		t.Errorf("expected no error here")
	}
	if schema.GetRowSizeBytes() != 16 {
		t.Error("Expected row size to be 16 bytes, got %d", schema.GetRowSizeBytes())
	}
}
