package clients

import (
	"errors"
	"testing"

	"github.com/kamva/octopus/base"
	. "github.com/kamva/octopus/clients/internal"
	"github.com/kamva/octopus/term"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ----------------------
//    Helper functions
// ----------------------

var errTest = errors.New("something went wrong")

type sqlOpener func(d string, u string) (base.SQLDatabase, error)

type dbQuerier func(db base.SQLDatabase, query string) (base.SQLRows, error)

var sqlOpenMock = func(d string, u string, sqlDB *SQLDatabase, err error) sqlOpener {
	return func(d string, u string) (base.SQLDatabase, error) {
		return sqlDB, err
	}
}

var queryDBMock = func(db base.SQLDatabase, query string, rows base.SQLRows) dbQuerier {
	return func(db base.SQLDatabase, query string) (base.SQLRows, error) {
		_, err := db.Query(query)
		return rows, err
	}
}

func initSQLServer(session base.SQLDatabase) *SQLServer {
	return &SQLServer{session: session}
}

func getTableStructure() base.TableStructure {
	return base.TableStructure{
		{Name: "ID", Type: "INT", Options: "IDENTITY PRIMARY KEY"},
		{Name: "SID", Type: "TINYINT", Options: "IDENTITY(1,5)"},
		{Name: "ResourceID", Type: "SMALLINT", Options: "PRIMARY KEY CLUSTERED"},
		{Name: "UserID", Type: "BIGINT", Options: "PRIMARY KEY NONCLUSTERED"},
		{Name: "Name", Type: "nvarchar(100)", Options: "NULL UNIQUE"},
		{Name: "Email", Type: "NVARCHAR(MAX)", Options: "UNIQUE CLUSTERED"},
		{Name: "Code", Type: "REAL", Options: "NOT NULL UNIQUE NONCLUSTERED"},
		{Name: "Age", Type: "FLOAT", Options: "DEFAULT 1 CHECK (Age > 0)"},
		{Name: "Old", Type: "BIT"},
		{Name: "Unsigned", Type: "DECIMAL"},
	}
}

// ----------------
//    Unit Tests
// ----------------

func TestNewSQLServer(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		original := sqlOpen
		defer func() { sqlOpen = original }()

		db := new(SQLDatabase)
		url := "localhost:1433"
		sqlOpen = sqlOpenMock("sqlserver", url, db, nil)

		assert.NotPanics(t, func() {
			client := NewSQLServer(url)
			sql := client.(*SQLServer)

			assert.Equal(t, db, sql.session)
		})
	})

	t.Run("fail", func(t *testing.T) {
		original := sqlOpen
		defer func() { sqlOpen = original }()

		db := new(SQLDatabase)
		url := "localhost:1433"
		sqlOpen = sqlOpenMock("sqlserver", url, db, nil)

		assert.NotPanics(t, func() {
			client := NewSQLServer(url)
			sql := client.(*SQLServer)

			assert.Equal(t, db, sql.session)
		})
	})
}

func TestSQLServer_CreateTable(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		session := new(SQLDatabase)

		createQuery := "IF NOT EXISTS (" +
			"SELECT * FROM INFORMATION_SCHEMA.TABLES " +
			"WHERE TABLE_SCHEMA = N'dbo' AND TABLE_NAME = N'accounts'" +
			") BEGIN " +
			"CREATE TABLE dbo.accounts (" +
			"ID INT IDENTITY PRIMARY KEY, " +
			"SID TINYINT IDENTITY(1,5), " +
			"ResourceID SMALLINT PRIMARY KEY CLUSTERED, " +
			"UserID BIGINT PRIMARY KEY NONCLUSTERED, " +
			"Name nvarchar(100) NULL UNIQUE, " +
			"Email NVARCHAR(MAX) UNIQUE CLUSTERED, " +
			"Code REAL NOT NULL UNIQUE NONCLUSTERED, " +
			"Age FLOAT DEFAULT 1 CHECK (Age > 0), " +
			"Old BIT, " +
			"Unsigned DECIMAL" +
			") END"

		session.On("Exec", createQuery).Return(nil, nil)

		client := initSQLServer(session)
		err := client.CreateTable("dbo.accounts", getTableStructure())

		assert.Nil(t, err)
	})

	t.Run("invalidTableName", func(t *testing.T) {
		session := new(SQLDatabase)

		client := initSQLServer(session)

		assert.Panics(t, func() {
			_ = client.CreateTable("invalidTableName", getTableStructure())
		})
	})

	t.Run("dbExecError", func(t *testing.T) {
		session := new(SQLDatabase)

		session.On("Exec", mock.AnythingOfType("string")).Return(nil, errTest)

		client := initSQLServer(session)
		err := client.CreateTable("dbo.accounts", getTableStructure())

		assert.NotNil(t, err)
	})
}

func TestSQLServer_EnsureIndex(t *testing.T) {
	t.Run("singleColumnIndex", func(t *testing.T) {
		session := new(SQLDatabase)

		query := "IF NOT EXISTS (" +
			"SELECT * FROM sys.indexes " +
			"WHERE name = N'Name_index' AND object_id = OBJECT_ID(N'dbo.accounts')" +
			") BEGIN CREATE INDEX Name_index ON dbo.accounts (Name) END"

		session.On("Exec", query).Return(nil, nil)

		client := initSQLServer(session)
		err := client.EnsureIndex("dbo.accounts", base.Index{
			Columns: []string{"Name"},
		})

		assert.Nil(t, err)
	})

	t.Run("multiColumnIndex", func(t *testing.T) {
		session := new(SQLDatabase)

		query := "IF NOT EXISTS (" +
			"SELECT * FROM sys.indexes " +
			"WHERE name = N'Name_Email_index' AND object_id = OBJECT_ID(N'dbo.accounts')" +
			") BEGIN CREATE INDEX Name_Email_index ON dbo.accounts (Name, Email) END"

		session.On("Exec", query).Return(nil, nil)

		client := initSQLServer(session)
		err := client.EnsureIndex("dbo.accounts", base.Index{
			Columns: []string{"Name", "Email"},
		})

		assert.Nil(t, err)
	})

	t.Run("singleColumnUniqueIndex", func(t *testing.T) {
		session := new(SQLDatabase)

		query := "IF NOT EXISTS (" +
			"SELECT * FROM sys.indexes " +
			"WHERE name = N'Name_unique_index' AND object_id = OBJECT_ID(N'dbo.accounts')" +
			") BEGIN CREATE UNIQUE INDEX Name_unique_index ON dbo.accounts (Name) END"

		session.On("Exec", query).Return(nil, nil)

		client := initSQLServer(session)
		err := client.EnsureIndex("dbo.accounts", base.Index{
			Columns: []string{"Name"},
			Unique:  true,
		})

		assert.Nil(t, err)
	})

	t.Run("multiColumnUniqueIndex", func(t *testing.T) {
		session := new(SQLDatabase)

		query := "IF NOT EXISTS (" +
			"SELECT * FROM sys.indexes " +
			"WHERE name = N'Name_Email_unique_index' AND object_id = OBJECT_ID(N'dbo.accounts')" +
			") BEGIN CREATE UNIQUE INDEX Name_Email_unique_index ON dbo.accounts (Name, Email) END"

		session.On("Exec", query).Return(nil, nil)

		client := initSQLServer(session)
		err := client.EnsureIndex("dbo.accounts", base.Index{
			Columns: []string{"Name", "Email"},
			Unique:  true,
		})

		assert.Nil(t, err)
	})

	t.Run("error", func(t *testing.T) {
		session := new(SQLDatabase)

		session.On("Exec", mock.AnythingOfType("string")).
			Return(nil, errTest)

		client := initSQLServer(session)
		err := client.EnsureIndex("dbo.accounts", base.Index{
			Columns: []string{"Name"},
		})

		assert.NotNil(t, err)
	})
}

func TestSQLServer_Insert(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "INSERT INTO dbo.players (name, rate, available) " +
			"OUTPUT inserted.* VALUES (N'Test', 3.5, 1)"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, nil)
		rows := new(SQLRows)

		rows.On("Next").Return(true)
		rows.On("Columns").Return(
			[]string{"id", "name", "rate", "available"},
			nil,
		)

		rows.On("Scan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				values := []interface{}{1, "Test", 3.5, true}
				for i, value := range values {
					arg := args.Get(i).(*interface{})
					*arg = value
				}
			})

		queryDB = queryDBMock(session, query, rows)
		client := initSQLServer(session)
		data := base.NewRecordData(
			[]string{"name", "rate", "available"},
			base.RecordMap{"name": "Test", "rate": 3.5, "available": true},
		)
		err := client.Insert("dbo.players", data)

		assert.Nil(t, err)

		assert.Equal(t, 1, data.Get("id"))
		assert.Equal(t, "Test", data.Get("name"))
		assert.Equal(t, 3.5, data.Get("rate"))
		assert.Equal(t, true, data.Get("available"))
	})

	t.Run("success", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "INSERT INTO dbo.players (name, rate, available) " +
			"OUTPUT inserted.* VALUES (N'Test', 3.5, 0)"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, nil)
		rows := new(SQLRows)

		rows.On("Next").Return(true)
		rows.On("Columns").Return(
			[]string{"id", "name", "rate", "available"},
			nil,
		)

		rows.On("Scan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				values := []interface{}{1, "Test", 3.5, false}
				for i, value := range values {
					arg := args.Get(i).(*interface{})
					*arg = value
				}
			})

		queryDB = queryDBMock(session, query, rows)
		client := initSQLServer(session)
		data := base.NewRecordData(
			[]string{"name", "rate", "available"},
			base.RecordMap{"name": "Test", "rate": 3.5, "available": false},
		)
		err := client.Insert("dbo.players", data)

		assert.Nil(t, err)

		assert.Equal(t, 1, data.Get("id"))
		assert.Equal(t, "Test", data.Get("name"))
		assert.Equal(t, 3.5, data.Get("rate"))
		assert.Equal(t, false, data.Get("available"))
	})

	t.Run("invalidDataType", func(t *testing.T) {
		session := new(SQLDatabase)
		client := initSQLServer(session)
		data := base.NewRecordData(
			[]string{"name", "rate", "invalid"},
			base.RecordMap{"name": "Test", "rate": 3.5, "invalid": base.RecordMap{"A": "a", "B": "b"}},
		)

		assert.Panics(t, func() {
			_ = client.Insert("dbo.players", data)
		})
	})

	t.Run("queryError", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "INSERT INTO dbo.players (name, rate, available) " +
			"OUTPUT inserted.* VALUES (N'Test', 3.5, 1)"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, errTest)
		rows := new(SQLRows)

		queryDB = queryDBMock(session, query, rows)
		client := initSQLServer(session)
		data := base.NewRecordData(
			[]string{"name", "rate", "available"},
			base.RecordMap{"name": "Test", "rate": 3.5, "available": true},
		)
		err := client.Insert("dbo.players", data)

		assert.NotNil(t, err)
	})

	t.Run("scanError", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "INSERT INTO dbo.players (name, rate, available) " +
			"OUTPUT inserted.* VALUES (N'Test', 3.5, 1)"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, nil)
		rows := new(SQLRows)

		rows.On("Next").Return(true)
		rows.On("Columns").Return(
			[]string{"id", "name", "rate", "available"},
			nil,
		)
		rows.On("Scan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errTest)

		queryDB = queryDBMock(session, query, rows)
		client := initSQLServer(session)
		data := base.NewRecordData(
			[]string{"name", "rate", "available"},
			base.RecordMap{"name": "Test", "rate": 3.5, "available": true},
		)
		err := client.Insert("dbo.players", data)

		assert.NotNil(t, err)
	})
}

func TestSQLServer_FindByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "SELECT * FROM dbo.players WHERE ID = 1"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, nil)
		rows := new(SQLRows)
		rows.On("Next").Return(true)
		rows.On("Columns").Return(
			[]string{"id", "name", "rate", "available"},
			nil,
		)
		rows.On("Scan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				values := []interface{}{1, "Test", 3.5, true}
				for i, value := range values {
					arg := args.Get(i).(*interface{})
					*arg = value
				}
			})

		queryDB = queryDBMock(session, query, rows)
		client := initSQLServer(session)
		data, err := client.FindByID("dbo.players", 1)

		assert.Nil(t, err)

		assert.Equal(t, 1, data.Get("id"))
		assert.Equal(t, "Test", data.Get("name"))
		assert.Equal(t, 3.5, data.Get("rate"))
		assert.Equal(t, true, data.Get("available"))
	})

	t.Run("notFound", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "SELECT * FROM dbo.players WHERE ID = 1"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, nil)
		rows := new(SQLRows)
		rows.On("Next").Return(false)

		queryDB = queryDBMock(session, query, rows)
		client := initSQLServer(session)
		data, err := client.FindByID("dbo.players", 1)

		assert.NotNil(t, err)
		assert.Equal(t, *base.ZeroRecordData(), data)
	})

	t.Run("queryError", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "SELECT * FROM dbo.players WHERE ID = 1"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, errTest)
		rows := new(SQLRows)

		queryDB = queryDBMock(session, query, rows)
		client := initSQLServer(session)
		data, err := client.FindByID("dbo.players", 1)

		assert.NotNil(t, err)
		assert.Equal(t, *base.ZeroRecordData(), data)
	})
}

func TestSQLServer_UpdateByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		query := "UPDATE dbo.players SET name = N'Updated Test', available = 0 WHERE ID = 1"

		session := new(SQLDatabase)
		session.On("Exec", query).Return(nil, nil)

		client := initSQLServer(session)
		data := base.NewRecordData(
			[]string{"name", "available"},
			base.RecordMap{"name": "Updated Test", "available": 0},
		)
		err := client.UpdateByID("dbo.players", 1, *data)

		assert.Nil(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		query := "UPDATE dbo.players SET name = N'Updated Test', rate = 9.1 WHERE ID = 1"

		session := new(SQLDatabase)
		session.On("Exec", query).Return(nil, errTest)

		client := initSQLServer(session)
		data := base.NewRecordData(
			[]string{"name", "rate"},
			base.RecordMap{"name": "Updated Test", "rate": 9.1},
		)
		err := client.UpdateByID("dbo.players", 1, *data)

		assert.NotNil(t, err)
	})
}

func TestSQLServer_DeleteByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		query := "DELETE FROM dbo.players WHERE ID = 1"

		session := new(SQLDatabase)
		session.On("Exec", query).Return(nil, nil)

		client := initSQLServer(session)
		err := client.DeleteByID("dbo.players", 1)

		assert.Nil(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		query := "DELETE FROM dbo.players WHERE ID = 1"

		session := new(SQLDatabase)
		session.On("Exec", query).Return(nil, errTest)

		client := initSQLServer(session)
		err := client.DeleteByID("dbo.players", 1)

		assert.NotNil(t, err)
	})
}

func TestSQLServer_Query(t *testing.T) {
	conditions := []base.Condition{
		term.Equal{Field: "name", Value: "Test"},
	}

	session := new(SQLDatabase)
	client := initSQLServer(session)
	r := client.Query("dbo.players", conditions...)

	assert.IsType(t, new(sqlQuery), r)

	q := r.(*sqlQuery)

	assert.Equal(t, conditions, q.conditions)
	assert.Equal(t, "dbo.players", q.table)
}

func TestSQLServer_Close(t *testing.T) {
	session := new(SQLDatabase)
	session.On("Close").Return(nil)
	client := initSQLServer(session)
	client.Close()

	assert.Nil(t, client.session)
}
