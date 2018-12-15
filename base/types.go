package base

import (
	"fmt"
	"strings"
)

// Enquoter is function alias for clients enquoting operation
type Enquoter func(i interface{}) string

// Index is a struct for declaring columns to be indexed.
// Indexes can have multiple columns (composite index)
// and can be defined as unique index.
type Index struct {
	// Column or columns to be index
	Columns []string

	// Determine the selected column or columns should be treated
	// as an unique index. note that if you set `Columns` with
	// multiple columns, a composite unique key will be created.
	Unique bool
}

// FieldStructure is representing a field structure in a table
type FieldStructure struct {
	Name     string
	Type     string
	Options  string
	stringer func(FieldStructure) string
}

func (s FieldStructure) String() string {
	if s.stringer != nil {
		return s.stringer(s)
	}

	return strings.TrimRight(fmt.Sprintf("%s %s %s", s.Name, s.Type, s.Options), " ")
}

// TableStructure is representing structure of a table fields
type TableStructure []FieldStructure

// GetInfo convert TableStructure to string value
func (t TableStructure) GetInfo() interface{} {
	var s = make([]string, 0)
	for _, field := range t {
		s = append(s, field.String())
	}

	return strings.Join(s, ", ")
}

// Sort is a struct for declaring result sort. It contains Column
// which is column/field name and Descending which determine
// the sort of results. result will sort Ascending by default
type Sort struct {
	// Column is the name of column to order the results by
	Column string

	// Descending determine sort is descending or ascending
	Descending bool
}
