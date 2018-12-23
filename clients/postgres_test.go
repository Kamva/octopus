package clients

import (
	"testing"

	"github.com/Kamva/octopus/term"

	"github.com/Kamva/octopus/base"
	"github.com/stretchr/testify/mock"

	. "github.com/Kamva/octopus/clients/internal"
	"github.com/stretchr/testify/assert"
)

// ----------------------
//    Helper functions
// ----------------------

func getPGTableStructure() base.TableStructure {
	return base.TableStructure{
		{Name: "id", Type: "SERIAL", Options: "PRIMARY KEY NOT NULL"},
		{Name: "name", Type: "VARCHAR(255)", Options: "NOT NULL"},
		{Name: "age", Type: "INT", Options: "NULL"},
		{Name: "status", Type: "BOOLEAN", Options: "DEFAULT TRUE"},
	}
}

func initPostgres(session base.SQLDatabase) *Postgres {
	return &Postgres{session: session}
}

// ----------------
//    Unit Tests
// ----------------

func TestNewPostgres(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		original := sqlOpen
		defer func() { sqlOpen = original }()

		db := new(SQLDatabase)
		url := "localhost:5432"
		sqlOpen = sqlOpenMock("postgres", url, db, nil)

		assert.NotPanics(t, func() {
			client := NewPostgres(url)
			sql := client.(*Postgres)

			assert.Equal(t, db, sql.session)
		})
	})

	t.Run("fail", func(t *testing.T) {
		original := sqlOpen
		defer func() { sqlOpen = original }()

		db := new(SQLDatabase)
		url := "invalid URL"
		sqlOpen = sqlOpenMock("postgres", url, db, errTest)

		assert.Panics(t, func() {
			_ = NewPostgres(url)
		})
	})
}

func TestPostgres_CreateTable(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		session := new(SQLDatabase)

		createQuery := "CREATE TABLE IF NOT EXISTS users ( " +
			"id SERIAL PRIMARY KEY NOT NULL, " +
			"name VARCHAR(255) NOT NULL, " +
			"age INT NULL, " +
			"status BOOLEAN DEFAULT TRUE )"

		session.On("Exec", createQuery).Return(nil, nil)

		client := initPostgres(session)
		err := client.CreateTable("users", getPGTableStructure())

		assert.Nil(t, err)
	})

	t.Run("dbExecError", func(t *testing.T) {
		session := new(SQLDatabase)

		session.On("Exec", mock.AnythingOfType("string")).Return(nil, errTest)

		client := initPostgres(session)
		err := client.CreateTable("users", getSQLTableStructure())

		assert.NotNil(t, err)
	})
}

func TestPostgres_EnsureIndex(t *testing.T) {
	t.Run("singleColumnIndex", func(t *testing.T) {
		session := new(SQLDatabase)

		query := "CREATE INDEX IF NOT EXISTS name_index ON users (name)"

		session.On("Exec", query).Return(nil, nil)

		client := initPostgres(session)
		err := client.EnsureIndex("users", base.Index{
			Columns: []string{"name"},
		})

		assert.Nil(t, err)
	})

	t.Run("multiColumnIndex", func(t *testing.T) {
		session := new(SQLDatabase)

		query := "CREATE INDEX IF NOT EXISTS name_email_index ON users (name, email)"

		session.On("Exec", query).Return(nil, nil)

		client := initPostgres(session)
		err := client.EnsureIndex("users", base.Index{
			Columns: []string{"name", "email"},
		})

		assert.Nil(t, err)
	})

	t.Run("singleColumnUniqueIndex", func(t *testing.T) {
		session := new(SQLDatabase)

		query := "CREATE UNIQUE INDEX IF NOT EXISTS name_unique_index ON users (name)"

		session.On("Exec", query).Return(nil, nil)

		client := initPostgres(session)
		err := client.EnsureIndex("users", base.Index{
			Columns: []string{"name"},
			Unique:  true,
		})

		assert.Nil(t, err)
	})

	t.Run("multiColumnUniqueIndex", func(t *testing.T) {
		session := new(SQLDatabase)

		query := "CREATE UNIQUE INDEX IF NOT EXISTS name_email_unique_index ON users (name, email)"

		session.On("Exec", query).Return(nil, nil)

		client := initPostgres(session)
		err := client.EnsureIndex("users", base.Index{
			Columns: []string{"name", "email"},
			Unique:  true,
		})

		assert.Nil(t, err)
	})

	t.Run("error", func(t *testing.T) {
		session := new(SQLDatabase)

		session.On("Exec", mock.AnythingOfType("string")).
			Return(nil, errTest)

		client := initPostgres(session)
		err := client.EnsureIndex("users", base.Index{
			Columns: []string{"name"},
		})

		assert.NotNil(t, err)
	})
}

func TestPostgres_Insert(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "INSERT INTO users (name, age, status) VALUES ('Test', 5, true) RETURNING *"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, nil)
		rows := new(SQLRows)

		rows.On("Next").Return(true)
		rows.On("Columns").Return(
			[]string{"id", "name", "age", "status"},
			nil,
		)

		rows.On("Scan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				values := []interface{}{1, "Test", 5, true}
				for i, value := range values {
					arg := args.Get(i).(*interface{})
					*arg = value
				}
			})

		queryDB = queryDBMock(session, query, rows)
		client := initPostgres(session)
		data := base.NewRecordData(
			[]string{"name", "age", "status"},
			base.RecordMap{"name": "Test", "age": 5, "status": true},
		)
		err := client.Insert("users", data)

		assert.Nil(t, err)

		assert.Equal(t, 1, data.Get("id"))
		assert.Equal(t, "Test", data.Get("name"))
		assert.Equal(t, 5, data.Get("age"))
		assert.Equal(t, true, data.Get("status"))
	})

	t.Run("successArrayAndMap", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "INSERT INTO users (number_slice, map_slice, string_slice, json) VALUES " +
			"('{2,3,5,7}', array['{\"a\":\"b\"}','{\"c\":\"d\"}']::json[], '{\"a\",\"b\"}', '{\"e\":\"f\"}') RETURNING *"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, nil)
		rows := new(SQLRows)

		rows.On("Next").Return(true)
		rows.On("Columns").Return(
			[]string{"id", "number_slice", "map_slice", "string_slice", "json"},
			nil,
		)

		rows.On("Scan", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				values := []interface{}{
					1, []byte("{2,3,5,7}"), []byte("{{\"a\":\"b\"},{\"c\":\"d\"}}"),
					[]byte("{\"a\",\"b\"}"), []byte("{\"e\":\"f\"}"),
				}
				for i, value := range values {
					arg := args.Get(i).(*interface{})
					*arg = value
				}
			})

		queryDB = queryDBMock(session, query, rows)
		client := initPostgres(session)
		data := base.NewRecordData(
			[]string{"number_slice", "map_slice", "string_slice", "json"},
			base.RecordMap{
				"number_slice": []int{2, 3, 5, 7},
				"map_slice":    []map[string]string{{"a": "b"}, {"c": "d"}},
				"string_slice": []string{"a", "b"},
				"json":         map[string]string{"e": "f"},
			},
		)
		err := client.Insert("users", data)

		assert.Nil(t, err)

		assert.Equal(t, 1, data.Get("id"))
		assert.Equal(t, "{2,3,5,7}", data.Get("number_slice"))
		assert.Equal(t, `{{"a":"b"},{"c":"d"}}`, data.Get("map_slice"))
		assert.Equal(t, `{"a","b"}`, data.Get("string_slice"))
		assert.Equal(t, `{"e":"f"}`, data.Get("json"))
	})

	t.Run("unsupportedType", func(t *testing.T) {
		session := new(SQLDatabase)
		client := initPostgres(session)
		data := base.NewRecordData(
			[]string{"invalidType"},
			base.RecordMap{
				"invalidType": func() {},
			},
		)
		assert.Panics(t, func() {
			_ = client.Insert("users", data)
		})
	})

	t.Run("unsupportedSlice", func(t *testing.T) {
		session := new(SQLDatabase)
		client := initPostgres(session)
		data := base.NewRecordData(
			[]string{"invalidSlice"},
			base.RecordMap{
				"invalidSlice": []chan int{make(chan int), make(chan int)},
			},
		)
		assert.Panics(t, func() {
			_ = client.Insert("users", data)
		})
	})

	t.Run("queryError", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "INSERT INTO users (name, age, status) VALUES ('Test', 5, true) RETURNING *"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, errTest)
		rows := new(SQLRows)

		queryDB = queryDBMock(session, query, rows)
		client := initPostgres(session)

		data := base.NewRecordData(
			[]string{"name", "age", "status"},
			base.RecordMap{"name": "Test", "age": 5, "status": true},
		)
		err := client.Insert("users", data)

		assert.NotNil(t, err)
	})
}

func TestPostgres_FindByID(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "SELECT * FROM users WHERE id = 1"

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
		client := initPostgres(session)
		data, err := client.FindByID("users", 1)

		assert.Nil(t, err)

		assert.Equal(t, 1, data.Get("id"))
		assert.Equal(t, "Test", data.Get("name"))
		assert.Equal(t, 3.5, data.Get("rate"))
		assert.Equal(t, true, data.Get("available"))
	})

	t.Run("notFound", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "SELECT * FROM users WHERE id = 1"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, nil)
		rows := new(SQLRows)
		rows.On("Next").Return(false)

		queryDB = queryDBMock(session, query, rows)
		client := initPostgres(session)
		data, err := client.FindByID("users", 1)

		assert.NotNil(t, err)
		assert.Equal(t, *base.ZeroRecordData(), data)
	})

	t.Run("queryError", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		query := "SELECT * FROM users WHERE id = 1"

		session := new(SQLDatabase)
		session.On("Query", query).Return(nil, errTest)
		rows := new(SQLRows)

		queryDB = queryDBMock(session, query, rows)
		client := initPostgres(session)
		data, err := client.FindByID("users", 1)

		assert.NotNil(t, err)
		assert.Equal(t, *base.ZeroRecordData(), data)
	})
}

func TestPostgres_UpdateByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		query := "UPDATE users SET name = 'Updated Test', available = false WHERE id = 1"

		session := new(SQLDatabase)
		session.On("Exec", query).Return(nil, nil)

		client := initPostgres(session)
		data := base.NewRecordData(
			[]string{"name", "available"},
			base.RecordMap{"name": "Updated Test", "available": false},
		)
		err := client.UpdateByID("users", 1, *data)

		assert.Nil(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		query := "UPDATE users SET name = 'Updated Test', rate = 9.1 WHERE id = 1"

		session := new(SQLDatabase)
		session.On("Exec", query).Return(nil, errTest)

		client := initPostgres(session)
		data := base.NewRecordData(
			[]string{"name", "rate"},
			base.RecordMap{"name": "Updated Test", "rate": 9.1},
		)
		err := client.UpdateByID("users", 1, *data)

		assert.NotNil(t, err)
	})
}

func TestPostgres_DeleteByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		query := "DELETE FROM users WHERE id = 1"

		session := new(SQLDatabase)
		session.On("Exec", query).Return(nil, nil)

		client := initPostgres(session)
		err := client.DeleteByID("users", 1)

		assert.Nil(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		query := "DELETE FROM users WHERE id = 1"

		session := new(SQLDatabase)
		session.On("Exec", query).Return(nil, errTest)

		client := initPostgres(session)
		err := client.DeleteByID("users", 1)

		assert.NotNil(t, err)
	})
}

func TestPostgres_Query(t *testing.T) {
	conditions := []base.Condition{
		term.Equal{Field: "name", Value: "Test"},
	}

	session := new(SQLDatabase)
	client := initPostgres(session)
	r := client.Query("users", conditions...)

	assert.IsType(t, new(sqlQuery), r)

	q := r.(*sqlQuery)

	assert.Equal(t, conditions, q.conditions)
	assert.Equal(t, "users", q.table)
}

func TestPostgres_Close(t *testing.T) {
	session := new(SQLDatabase)
	session.On("Close").Return(nil)
	client := initPostgres(session)
	client.Close()

	assert.Nil(t, client.session)
}
