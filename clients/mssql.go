package clients

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/Kamva/nautilus/excp"
	"github.com/Kamva/octopus/base"

	// Register mssql driver to database/sql So you can use
	// sql.Open("sqlserver", ...) to open postgres connection session
	_ "github.com/denisenkom/go-mssqldb"
)

// SQLServer is the Microsoft SQL Server session
type SQLServer struct {
	session base.SQLDatabase
}

// CreateTable creates `tableName` table with field and structure
// defined in `structure` parameter for each table fields
func (c *SQLServer) CreateTable(tableName string, info base.TableInfo) error {
	existenceCheckQuery := c.generateTableExistenceCheckQuery(tableName)
	createQuery := c.generateCreateQuery(tableName, info)

	_, err := c.session.Exec(fmt.Sprintf(
		"IF NOT EXISTS (%s) BEGIN %s END",
		existenceCheckQuery, createQuery,
	))

	return err
}

// EnsureIndex ensures that `index` is exists on `tableName` table,
// if not, it tries to create index with specified condition in
// `index` on `tableName`.
func (c *SQLServer) EnsureIndex(tableName string, index base.Index) error {
	columns := strings.Join(index.Columns, ", ")

	var indexName, createQuery string
	if index.Unique {
		indexName = fmt.Sprintf(
			"%s_unique_index",
			strings.Join(index.Columns, "_"),
		)

		createQuery = fmt.Sprintf(
			"CREATE UNIQUE INDEX %s ON %s (%s)",
			indexName, tableName, columns,
		)
	} else {
		indexName = fmt.Sprintf(
			"%s_index",
			strings.Join(index.Columns, "_"),
		)

		createQuery = fmt.Sprintf(
			"CREATE INDEX %s ON %s (%s)",
			indexName, tableName, columns,
		)
	}

	existenceCheckQuery := fmt.Sprintf(
		"SELECT * FROM sys.indexes WHERE name = %s AND object_id = OBJECT_ID(%s)",
		c.enquoteValue(indexName), c.enquoteValue(tableName),
	)

	_, err := c.session.Exec(fmt.Sprintf(
		"IF NOT EXISTS (%s) BEGIN %s END",
		existenceCheckQuery, createQuery,
	))

	return err
}

// Insert tries to insert `data` into `tableName` and returns error if
// anything went wrong. `data` should pass by reference to have exact
// data on `tableName`, otherwise updated record data isn't accessible.
func (c *SQLServer) Insert(tableName string, data *base.RecordData) error {
	strings.Join(data.GetColumns(), ", ")
	rows, err := queryDB(c.session, fmt.Sprintf(
		"INSERT INTO %s (%s) OUTPUT inserted.* VALUES (%s)",
		tableName,
		strings.Join(data.GetColumns(), ", "),
		strings.Join(data.GetValues(c.enquoteValue), ", "),
	))

	if err != nil {
		return err
	}

	return fetchSingleRecord(rows, data)
}

// FindByID searches through `tableName` records to find a row that its
// ID match with `id` and returns it alongside any possible error.
func (c *SQLServer) FindByID(tableName string, id interface{}) (base.RecordData, error) {
	data := *base.ZeroRecordData()
	rows, err := queryDB(c.session, fmt.Sprintf(
		"SELECT * FROM %s WHERE ID = %v",
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
func (c *SQLServer) UpdateByID(tableName string, id interface{}, data base.RecordData) error {
	updateQuery := prepareUpdate(data, c.enquoteValue)
	_, err := c.session.Exec(fmt.Sprintf(
		"UPDATE %s SET %s WHERE ID = %v",
		tableName, updateQuery, id,
	))

	return err
}

// DeleteByID finds a record in `tableName` that its ID match with `id`,
// and remove it entirely. It will return error if anything went wrong.
func (c *SQLServer) DeleteByID(tableName string, id interface{}) error {
	_, err := c.session.Exec(fmt.Sprintf(
		"DELETE FROM %s WHERE ID = %v",
		tableName, id,
	))

	return err
}

// Query generates and returns sqlQuery object for further operations
func (c *SQLServer) Query(tableName string, conditions ...base.Condition) base.QueryBuilder {
	return newSQLQuery(c.session, tableName, conditions, c.enquoteValue)
}

// Close disconnect session from database and release the taken memory
func (c *SQLServer) Close() {
	_ = c.session.Close()
	c.session = nil
}

// Generate sqlQuery that search given table with given schema
func (c *SQLServer) generateTableExistenceCheckQuery(table string) string {
	parts := strings.Split(table, ".")

	if len(parts) != 2 {
		panic(fmt.Sprintf(
			"Invalid table name [%s]. Table name should be in [schema].[tablename] format.",
			table,
		))
	}

	return fmt.Sprintf(
		"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
		c.enquoteValue(parts[0]), c.enquoteValue(parts[1]),
	)
}

// Enquote values to a proper presentation of their type in sql string
func (c *SQLServer) enquoteValue(i interface{}) string {
	t := reflect.TypeOf(i)

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%v", i)
	case reflect.String:
		return fmt.Sprintf("N'%s'", i.(string))
	case reflect.Bool:
		b := i.(bool)
		if b {
			return "1"
		}
		return "0"
	}

	panic(fmt.Sprintf("Value with type of %s is not supported", t.Kind().String()))
}

func (c *SQLServer) generateCreateQuery(table string, info base.TableInfo) string {
	return fmt.Sprintf("CREATE TABLE %s (%s)", table, info.GetInfo().(string))
}

// NewSQLServer instantiate and return a new SQLServer session object
func NewSQLServer(url string) base.Client {
	session, err := sqlOpen("sqlserver", url)
	excp.PanicIfErr(err)

	return &SQLServer{session: session}
}

// sqlOpen open a connection to given url by given driver.
// This is separated as a variable to mocked easily.
var sqlOpen = func(driver string, url string) (base.SQLDatabase, error) {
	return sql.Open(driver, url)
}
