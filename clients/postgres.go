package clients

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/kamva/nautilus/excp"
	"github.com/kamva/octopus/base"

	// Register pq postgres client to database/sql So you can use
	// sql.Open("postgres", ...) to open postgres connection session
	_ "github.com/lib/pq"
)

// Postgres is the PostgreSQL client
type Postgres struct {
	session base.SQLDatabase
}

// CreateTable creates `tableName` table with field and structure
// defined in `structure` parameter for each table fields
func (c *Postgres) CreateTable(tableName string, info base.TableInfo) error {
	_, err := c.session.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s ( %s )",
		tableName, info.GetInfo().(string),
	))

	return err
}

// EnsureIndex ensures that `index` is exists on `tableName` table,
// if not, it tries to create index with specified condition in
// `index` on `tableName`.
func (c *Postgres) EnsureIndex(tableName string, index base.Index) error {
	columns := strings.Join(index.Columns, ", ")

	var createQuery string
	if index.Unique {
		indexName := fmt.Sprintf(
			"%s_unique_index",
			strings.Join(index.Columns, "_"),
		)

		createQuery = fmt.Sprintf(
			"CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (%s)",
			indexName, tableName, columns,
		)
	} else {
		indexName := fmt.Sprintf(
			"%s_index",
			strings.Join(index.Columns, "_"),
		)

		createQuery = fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
			indexName, tableName, columns,
		)
	}

	_, err := c.session.Exec(createQuery)

	return err
}

// Insert tries to insert `data` into `tableName` and returns error if
// anything went wrong. `data` should pass by reference to have exact
// data on `tableName`, otherwise updated record data isn't accessible.
func (c *Postgres) Insert(tableName string, data *base.RecordData) error {
	strings.Join(data.GetColumns(), ", ")

	rows, err := queryDB(c.session, fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) RETURNING *",
		tableName,
		strings.Join(data.GetColumns(), ", "),
		strings.Join(data.GetValues(c.enquoteValue), ", "),
	))

	if err != nil {
		return err
	}

	err = fetchSingleRecord(rows, data)

	data.PruneData(func(recordMap *base.RecordMap) {
		maps := *recordMap
		for key, value := range maps {
			if v, ok := value.([]uint8); ok {
				(*recordMap)[key] = string(v)
			}
		}
	})

	return err
}

// FindByID searches through `tableName` records to find a row that its
// ID match with `id` and returns it alongside any possible error.
func (c *Postgres) FindByID(tableName string, id interface{}) (base.RecordData, error) {
	data := *base.ZeroRecordData()
	rows, err := queryDB(c.session, fmt.Sprintf(
		"SELECT * FROM %s WHERE id = %v",
		tableName, id,
	))

	if err != nil {
		return data, err
	}

	err = fetchSingleRecord(rows, &data)

	if err != nil {
		data.Zero()
		return data, err
	}

	return data, err
}

// UpdateByID finds a record in `tableName` that its ID match with `id`,
// and updates it with data. It will return error if anything went wrong.
func (c *Postgres) UpdateByID(tableName string, id interface{}, data base.RecordData) error {
	updateQuery := prepareUpdate(data, c.enquoteValue)
	_, err := c.session.Exec(fmt.Sprintf(
		"UPDATE %s SET %s WHERE id = %v",
		tableName, updateQuery, id,
	))

	return err
}

// DeleteByID finds a record in `tableName` that its ID match with `id`,
// and remove it entirely. It will return error if anything went wrong.
func (c *Postgres) DeleteByID(tableName string, id interface{}) error {
	_, err := c.session.Exec(fmt.Sprintf(
		"DELETE FROM %s WHERE id = %v",
		tableName, id,
	))

	return err
}

// Query generates and returns sqlQuery object for further operations
func (c *Postgres) Query(tableName string, conditions ...base.Condition) base.QueryBuilder {
	return newSQLQuery(c.session, tableName, conditions, c.enquoteValue)
}

// Close disconnect session from database and release the taken memory
func (c *Postgres) Close() {
	_ = c.session.Close()
	c.session = nil
}

// Enquote values to a proper presentation of their type in sql string
func (c *Postgres) enquoteValue(i interface{}) string {
	t := reflect.TypeOf(i)

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Bool:
		return fmt.Sprintf("%v", i)
	case reflect.Array, reflect.Slice:
		return c.enquoteSliceValue(i)
	case reflect.Map, reflect.Struct:
		bytes, err := json.Marshal(i)
		excp.PanicIfErr(err)
		return fmt.Sprintf("'%s'", string(bytes))
	case reflect.String:
		return fmt.Sprintf("'%s'", i.(string))
	}

	panic(fmt.Sprintf("Value with type of %s is not supported", t.Kind().String()))
}

// Enquote arrays and slices to a proper presentation of their type in sql string
func (c *Postgres) enquoteSliceValue(i interface{}) string {
	t := reflect.TypeOf(i).Elem()

	tmp := make([]string, 0)
	var slice []interface{}

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Bool:
		data, _ := json.Marshal(i)
		_ = json.Unmarshal(data, &slice)

		for _, item := range slice {
			tmp = append(tmp, fmt.Sprintf("%v", item))
		}

		return fmt.Sprintf("'{%s}'", strings.Join(tmp, ","))
	case reflect.Map, reflect.Struct:
		data, _ := json.Marshal(i)
		_ = json.Unmarshal(data, &slice)

		for _, item := range slice {
			bytes, err := json.Marshal(item)
			excp.PanicIfErr(err)
			tmp = append(tmp, fmt.Sprintf("'%s'", string(bytes)))
		}

		return fmt.Sprintf("array[%s]::json[]", strings.Join(tmp, ","))
	case reflect.String:
		for _, item := range i.([]string) {
			tmp = append(tmp, fmt.Sprintf("\"%s\"", item))
		}

		return fmt.Sprintf("'{%s}'", strings.Join(tmp, ","))
	}

	panic(fmt.Sprintf("Value with type of []%s is not supported", t.Kind().String()))
}

// NewPostgres instantiate and return a new PostgreSQL session object
func NewPostgres(url string) base.Client {
	session, err := sqlOpen("postgres", url)
	excp.PanicIfErr(err)

	return &Postgres{session: session}
}
