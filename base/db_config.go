package base

import "strings"

// driverName is string alias for determining driver name
type driverName string

const (
	// Mongo represent driver name for mongodb driver
	Mongo driverName = "mongo"

	// PG represent driver name for PostgreSQL
	PG driverName = "pg"

	// MSSQL represent driver name for Microsoft SQL Server
	MSSQL driverName = "mssql"
)

// DBConfig is the connection settings and options
type DBConfig struct {
	Driver   driverName
	Host     string
	Port     string
	Database string
	Username string
	Password string
	Prefix   string
	Options  map[string]string
}

// HasPrefix Check if any table/collection prefix is set
func (c *DBConfig) HasPrefix() bool {
	return c.Prefix != ""
}

// GetOptions return connection options as a querystring
func (c *DBConfig) GetOptions() (options string) {
	for key, value := range c.Options {
		options = options + key + "=" + value + "&"
	}

	return string(strings.TrimRight(options, "&"))
}

// AddOption append given key value to connection options map
func (c *DBConfig) AddOption(key string, value string) {
	if c.Options == nil {
		c.Options = make(map[string]string)
	}

	c.Options[key] = value
}
