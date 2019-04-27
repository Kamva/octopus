package base

// Client is an interface for database clients. Database clients are
// responsible with connecting and interacting with database instance.
type Client interface {

	// CreateTable creates `tableName` table with field and structure
	// defined in `structure` parameter for each table fields
	CreateTable(tableName string, info TableInfo) error

	// EnsureIndex ensures that `index` is exists on `tableName` table,
	// if not, it tries to create index with specified condition in
	// `index` on `tableName`.
	EnsureIndex(tableName string, index Index) error

	// Insert tries to insert `data` into `tableName` and returns error if
	// anything went wrong. `data` should pass by reference to have exact
	// data on `tableName`, otherwise updated record data isn't accessible.
	Insert(tableName string, data *RecordData) error

	// FindByID searches through `tableName` records to find a row that its
	// ID match with `id` and returns it alongside any possible error.
	FindByID(tableName string, id interface{}) (RecordData, error)

	// UpdateByID finds a record in `tableName` that its ID match with `id`,
	// and updates it with data. It will return error if anything went wrong.
	UpdateByID(tableName string, id interface{}, data RecordData) error

	// DeleteByID finds a record in `tableName` that its ID match with `id`,
	// and remove it entirely. It will return error if anything went wrong.
	DeleteByID(tableName string, id interface{}) error

	// Query generates and returns query object for further operations
	Query(tableName string, conditions ...Condition) QueryBuilder

	// Close disconnect client from database and release the taken memory
	Close()
}

// QueryBuilder is an object that contains information about query. With QueryBuilder
// you can fetch, update and delete records from database.
type QueryBuilder interface {

	// OrderBy set the order of returning result in following command
	OrderBy(sorts ...Sort) QueryBuilder

	// Limit set the limit that determines how many results should be
	// returned in the following fetch command.
	Limit(n int) QueryBuilder

	// Skip set the starting offset of the following fetch command
	Skip(n int) QueryBuilder

	// Count execute a count command that will return the number records in
	// specified destination table. If the query conditions was empty, it
	// returns number of all records un destination table.
	Count() (int, error)

	// First fetch data of the first record that match with query conditions.
	First() (RecordData, error)

	// All returns results that match with query conditions in RecordDataSet
	// format. If the query conditions was empty it will return all records
	// in specified destination table or error if anything went wrong.
	All() (RecordDataSet, error)

	// Update updates records that math with query conditions with `data` and
	// returns number of affected rows and error if anything went wring. If
	// the query condition was empty it'll update all records in destination
	// table.
	Update(data RecordData) (int, error)

	// Delete removes every records in destination table that match with condition
	// query and returns number of affected rows and error if anything went wrong.
	// It will removes all records inside destination table if no condition query
	// was set.
	Delete() (int, error)
}

// Condition is an interface for query conditions
type Condition interface {
	// GetField returns the name of field to for querying
	GetField() string

	// GetValue returns the value to be compared or checked in query
	GetValue() interface{}
}

// TableInfo is an interface used for data of a table or collection.
// it could be a table structure or collection info.
type TableInfo interface {
	GetInfo() interface{}
}

// Scheme is an interface represent a record or document in database
type Scheme interface {

	// GetID returns the value of the record identifier
	GetID() interface{}

	// GetKeyName return the name of primary key field name
	GetKeyName() string
}

// MsScheme is the same as Scheme except that it has one more
// method that is for getting table scheme name in database
type MsScheme interface {

	// Extend Scheme interface
	Scheme

	// GetSchema returns name of the table schema
	GetSchema() string
}

// Builder is a wrapper around QueryBuilder that convert RecordData object to
// model's related scheme.
type Builder interface {

	// OrderBy set the order of returning result in following command
	OrderBy(sorts ...Sort) Builder

	// Limit set the limit that determines how many results should be
	// returned in the following fetch command.
	Limit(n int) Builder

	// Skip set the starting offset of the following fetch command
	Skip(n int) Builder

	// Count execute a count command that will return the number records in
	// specified destination table. If the query conditions was empty, it
	// returns number of all records un destination table.
	Count() (int, error)

	// First fetch data of the first record that match with query conditions.
	First() (Scheme, error)

	// All returns results that match with query conditions in RecordDataSet
	// format. If the query conditions was empty it will return all records
	// in specified destination table or error if anything went wrong.
	All() ([]Scheme, error)

	// Update updates records that math with query conditions with `data` and
	// returns number of affected rows and error if anything went wring. If
	// the query condition was empty it'll update all records in destination
	// table.
	Update(data Scheme) (int, error)

	// Delete removes every records in destination table that match with condition
	// query and returns number of affected rows and error if anything went wrong.
	// It will removes all records inside destination table if no condition query
	// was set.
	Delete() (int, error)
}
