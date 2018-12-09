package base

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

// RecordData is map of string interface which represent data on
// a record in database. It could be used for both upserting and
// fetching data from database. Map key represents the column
// name and its value represents the column value
type RecordData map[string]interface{}

// RecordDataSet is slice of RecordData represents results from db
type RecordDataSet []RecordData

// FieldStructure is representing a field structure in a table
type FieldStructure struct {
	Name    string
	Type    string
	Options string
}

// TableStructure is representing structure of a table fields
type TableStructure []FieldStructure
