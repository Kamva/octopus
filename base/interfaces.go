package base

// Client is an interface for database clients. Database clients are
// responsible with connecting and interacting with database instance.
type Client interface {

	// CreateTable creates `tableName` table with field and structure
	// defined in `structure` parameter for each table fields
	CreateTable(tableName string, structure TableStructure) error

	// EnsureIndex ensures that `index` is exists on `tableName` table,
	// if not, it tries to create index with specified condition in
	// `index` on `tableName`.
	EnsureIndex(tableName string, index Index) error

	// Insert tries to insert `data` into `tableName` and returns error if
	// anything went wrong. `data` should pass by reference to have exact
	// data on `tableName`, otherwise updated record data isn't accessible.
	Insert(tableName string, data RecordData) error

	// FindByID searches through `tableName` records to find a row that its
	// ID match with `id` and returns it alongside any possible error.
	FindByID(tableName string, id interface{}) (RecordData, error)

	// UpdateByID finds a record in `tableName` that its ID match with `id`,
	// and updates it with data. It will return error if anything went wrong.
	UpdateByID(tableName string, id interface{}, data RecordData) error

	// DeleteByID finds a record in `tableName` that its ID match with `id`,
	// and remove it entirely. It will return error if anything went wrong.
	DeleteByID(tableName string, id interface{}) error

	// Query sets `conditions` for `tableName` in client for further operations.
	Query(tableName string, conditions ...Condition) Client

	// All returns results that match with query conditions in RecordDataSet
	// format. If the query conditions was empty it will return all records
	// in specified destination table or error if anything went wrong.
	// It will panic if no destination table was set before call All.
	All() (RecordDataSet, error)

	// Update updates records that math with query conditions with `data` and
	// returns number of affected rows and error if anything went wring. If
	// the query condition was empty it'll update all records in destination
	// table. And panics if no destination table was set before call Update.
	Update(data RecordData) (int, error)

	// Delete removes every records in destination table that match with condition
	// query and returns number of affected rows and error if anything went wrong.
	// It will removes all records inside destination table if no condition query
	// was set and panics if the destination table is not set before call Delete.
	Delete() (int, error)

	// Close disconnect client from database and release the taken memory
	Close()
}

// Condition is an interface for query conditions
type Condition interface {
	// GetField returns the name of field to for querying
	GetField() string

	// GetValue returns the value to be compared or checked in query
	GetValue() interface{}
}
