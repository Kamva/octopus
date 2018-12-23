package clients

import (
	"math/rand"
	"testing"

	"github.com/Kamva/octopus/base"
	. "github.com/Kamva/octopus/clients/internal"
	"github.com/Kamva/octopus/term"
	"github.com/icrowley/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ----------------------
//    Helper functions
// ----------------------

var conditions = []base.Condition{
	term.Equal{Field: "age", Value: 19},
	term.NotEqual{Field: "team", Value: "Manchester City"},
	term.GreaterThan{Field: "rate", Value: 8.5},
	term.GreaterThanEqual{Field: "score", Value: 10},
	term.LessThan{Field: "yellow_cards", Value: 2},
	term.LessThanEqual{Field: "red_cards", Value: 1},
	term.In{Field: "grade", Values: []interface{}{"A", "B"}},
	term.IsNull{Field: "banned_date"},
	term.NotNull{Field: "trophies"},
}

var simpleCondition = []base.Condition{
	term.Equal{Field: "name", Value: "Test"},
}

var tableName = "dbo.players"

func initQuery(db base.SQLDatabase, enquoter base.Enquoter) *sqlQuery {
	return &sqlQuery{session: db, table: tableName, conditions: conditions, enquoter: enquoter}
}

var teams = []string{"Manchester United", "Chelsea", "Arsenal", "Liverpool"}
var grades = []string{"A", "B"}

var columns = []string{
	"id", "name", "rate", "score", "age", "team", "yellow_cards",
	"red_cards", "grade", "banned_date", "trophies",
}

var simpleColumns = []string{"id", "name", "rate", "available"}

var recordGenerator = func(args mock.Arguments) {
	id := args.Get(0).(*interface{})
	*id = rand.Intn(100) + 1
	name := args.Get(1).(*interface{})
	*name = fake.FullName()
	rate := args.Get(2).(*interface{})
	*rate = (rand.Intn(15) + 86) / 10
	score := args.Get(3).(*interface{})
	*score = rand.Intn(30) + 10
	age := args.Get(4).(*interface{})
	*age = 19
	team := args.Get(5).(*interface{})
	*team = teams[rand.Intn(4)]
	yellows := args.Get(6).(*interface{})
	*yellows = rand.Intn(2)
	reds := args.Get(7).(*interface{})
	*reds = rand.Intn(1)
	grade := args.Get(8).(*interface{})
	*grade = grades[rand.Intn(2)]
	banned := args.Get(9).(*interface{})
	*banned = nil
	trophies := args.Get(10).(*interface{})
	*trophies = rand.Intn(5) + 1
}

type result struct {
	count int64
}

func (r result) LastInsertId() (int64, error) {
	return r.count, nil
}

func (r result) RowsAffected() (int64, error) {
	return r.count, nil
}

// ----------------
//    Unit Tests
// ----------------

func TestSqlQuery_OrderBy(t *testing.T) {
	sorts := []base.Sort{
		{Column: "name", Descending: true},
		{Column: "rate"},
	}

	session := new(SQLDatabase)
	query := initQuery(session, nil)
	q := query.OrderBy(sorts...)

	assert.IsType(t, query, q)

	c := q.(*sqlQuery)

	assert.Equal(t, sorts, c.sorts)
}

func TestSqlQuery_Limit(t *testing.T) {
	session := new(SQLDatabase)
	query := initQuery(session, nil)
	q := query.Limit(5)

	assert.IsType(t, query, q)

	c := q.(*sqlQuery)

	assert.Equal(t, 5, c.limit)
}

func TestSqlQuery_Skip(t *testing.T) {
	session := new(SQLDatabase)
	query := initQuery(session, nil)
	q := query.Skip(10)

	assert.IsType(t, query, q)

	c := q.(*sqlQuery)

	assert.Equal(t, 10, c.offset)
}

func TestSqlQuery_Count(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT COUNT(*) AS count FROM dbo.players"
		count := 946

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, nil)
		rows := new(SQLRows)
		rows.On("Next").Return(true)
		rows.On("Columns").Return([]string{"count"}, nil)
		rows.On("Scan", mock.Anything).Return(nil).
			Run(func(args mock.Arguments) {
				arg := args.Get(0).(*interface{})
				*arg = count
			})

		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, nil)
		n, err := query.Count()

		assert.Nil(t, err)
		assert.Equal(t, count, n)
	})

	t.Run("notFound", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT COUNT(*) AS count FROM dbo.players"
		count := 0

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, nil)
		rows := new(SQLRows)
		rows.On("Next").Return(true)
		rows.On("Columns").Return([]string{"count"}, nil)
		rows.On("Scan", mock.Anything).Return(nil).
			Run(func(args mock.Arguments) {
				arg := args.Get(0).(*interface{})
				*arg = count
			})

		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, nil)
		n, err := query.Count()

		assert.Nil(t, err)
		assert.Equal(t, count, n)
	})

	t.Run("queryError", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT COUNT(*) AS count FROM dbo.players"

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, errTest)
		rows := new(SQLRows)
		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, nil)
		n, err := query.Count()

		assert.NotNil(t, err)
		assert.Equal(t, 0, n)
	})
}

func TestSqlQuery_All(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT * FROM dbo.players WHERE " +
			"age = 19 AND team != N'Manchester City' AND rate > 8.5 AND score >= 10 AND " +
			"yellow_cards < 2 AND red_cards <= 1 AND grade IN (N'A', N'B') AND " +
			"banned_date IS NULL AND trophies IS NOT NULL"
		limit := 10

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, nil)
		rows := new(SQLRows)
		rows.SetLimit(limit)
		rows.On("Next").Return(true)
		rows.On("Columns").Return(columns, nil)
		args := make([]interface{}, 0, 11)
		for i := 0; i < 11; i++ {
			args = append(args, mock.Anything)
		}
		rows.On("Scan", args...).Return(nil).Run(recordGenerator)

		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, new(SQLServer).enquoteValue)
		results, err := query.All()

		assert.Nil(t, err)
		assert.Equal(t, limit, len(results))
	})

	t.Run("foundWithOptions", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT * FROM dbo.players WHERE " +
			"age = 19 AND team != N'Manchester City' AND rate > 8.5 AND score >= 10 AND " +
			"yellow_cards < 2 AND red_cards <= 1 AND grade IN (N'A', N'B') AND " +
			"banned_date IS NULL AND trophies IS NOT NULL LIMIT 10 OFFSET 50 " +
			"ORDER BY score DESC, grade ASC"
		limit := 10
		sorts := []base.Sort{
			{Column: "score", Descending: true},
			{Column: "grade", Descending: false},
		}

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, nil)
		rows := new(SQLRows)
		rows.SetLimit(limit)
		rows.On("Next").Return(true)
		rows.On("Columns").Return(columns, nil)
		args := make([]interface{}, 0, 11)
		for i := 0; i < 11; i++ {
			args = append(args, mock.Anything)
		}
		rows.On("Scan", args...).Return(nil).Run(recordGenerator)

		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, new(SQLServer).enquoteValue)
		results, err := query.Limit(limit).Skip(50).OrderBy(sorts...).All()

		assert.Nil(t, err)
		assert.Equal(t, limit, len(results))
	})

	t.Run("notFound", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT * FROM dbo.players WHERE " +
			"age = 19 AND team != N'Manchester City' AND rate > 8.5 AND score >= 10 AND " +
			"yellow_cards < 2 AND red_cards <= 1 AND grade IN (N'A', N'B') AND " +
			"banned_date IS NULL AND trophies IS NOT NULL"

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, nil)
		rows := new(SQLRows)
		rows.On("Next").Return(false)
		rows.On("Columns").Return(columns, nil)

		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, new(SQLServer).enquoteValue)
		results, err := query.All()

		assert.Nil(t, err)
		assert.Equal(t, 0, len(results))
	})

	t.Run("queryError", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT * FROM dbo.players WHERE " +
			"age = 19 AND team != N'Manchester City' AND rate > 8.5 AND score >= 10 AND " +
			"yellow_cards < 2 AND red_cards <= 1 AND grade IN (N'A', N'B') AND " +
			"banned_date IS NULL AND trophies IS NOT NULL"

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, errTest)
		rows := new(SQLRows)

		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, new(SQLServer).enquoteValue)
		results, err := query.All()

		assert.NotNil(t, err)
		assert.Equal(t, 0, len(results))
		assert.Nil(t, results)
	})

	t.Run("scanError", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT * FROM dbo.players WHERE " +
			"age = 19 AND team != N'Manchester City' AND rate > 8.5 AND score >= 10 AND " +
			"yellow_cards < 2 AND red_cards <= 1 AND grade IN (N'A', N'B') AND " +
			"banned_date IS NULL AND trophies IS NOT NULL"

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, nil)
		rows := new(SQLRows)
		rows.On("Next").Return(true)
		rows.On("Columns").Return(columns, nil)
		args := make([]interface{}, 0, 11)
		for i := 0; i < 11; i++ {
			args = append(args, mock.Anything)
		}
		rows.On("Scan", args...).Return(errTest)

		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, new(SQLServer).enquoteValue)
		results, err := query.All()

		assert.NotNil(t, err)
		assert.Equal(t, 0, len(results))
		assert.Nil(t, results)
	})
}

func TestSqlQuery_First(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT * FROM dbo.players WHERE name = N'Test' LIMIT 1"

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, nil)
		rows := new(SQLRows)
		rows.On("Next").Return(true)
		rows.On("Columns").Return(simpleColumns, nil)
		rows.On("Scan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				values := []interface{}{1, "Test", 3.5, true}
				for i, value := range values {
					arg := args.Get(i).(*interface{})
					*arg = value
				}
			})

		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, new(SQLServer).enquoteValue)
		query.conditions = simpleCondition
		data, err := query.First()

		assert.Nil(t, err)

		assert.Equal(t, 1, data.Get("id"))
		assert.Equal(t, "Test", data.Get("name"))
		assert.Equal(t, 3.5, data.Get("rate"))
		assert.Equal(t, true, data.Get("available"))
	})

	t.Run("notFound", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT * FROM dbo.players WHERE name = N'Test' LIMIT 1"

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, nil)
		rows := new(SQLRows)
		rows.On("Next").Return(false)
		rows.On("Columns").Return(simpleColumns, nil)

		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, new(SQLServer).enquoteValue)
		query.conditions = simpleCondition
		data, err := query.First()

		assert.NotNil(t, err)
		assert.Equal(t, 0, len(data.GetColumns()))
		assert.Equal(t, 0, len(data.GetValues(query.enquoter)))
	})

	t.Run("queryError", func(t *testing.T) {
		original := queryDB
		defer func() { queryDB = original }()

		sqlQuery := "SELECT * FROM dbo.players WHERE name = N'Test' LIMIT 1"

		session := new(SQLDatabase)
		session.On("Query", sqlQuery).Return(nil, errTest)
		rows := new(SQLRows)

		queryDB = queryDBMock(session, sqlQuery, rows)
		query := initQuery(session, new(SQLServer).enquoteValue)
		query.conditions = simpleCondition
		data, err := query.First()

		assert.NotNil(t, err)
		assert.Equal(t, 0, len(data.GetColumns()))
		assert.Equal(t, 0, len(data.GetValues(query.enquoter)))
	})
}

func TestSqlQuery_Update(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		sqlQuery := "UPDATE dbo.players SET " +
			"name = N'Updated Test', rate = 5.7 " +
			"WHERE name = N'Test'"
		changeDate := *base.NewRecordData(
			[]string{"name", "rate"},
			base.RecordMap{"name": "Updated Test", "rate": 5.7},
		)
		res := result{rand.Int63n(100)}

		session := new(SQLDatabase)
		session.On("Exec", sqlQuery).Return(res, nil)
		query := initQuery(session, new(SQLServer).enquoteValue)
		query.conditions = simpleCondition

		count, err := query.Update(changeDate)

		assert.Nil(t, err)
		assert.Equal(t, int(res.count), count)
	})

	t.Run("failed", func(t *testing.T) {
		sqlQuery := "UPDATE dbo.players SET " +
			"name = N'Updated Test', rate = 5.7 " +
			"WHERE name = N'Test'"
		changeDate := *base.NewRecordData(
			[]string{"name", "rate"},
			base.RecordMap{"name": "Updated Test", "rate": 5.7},
		)
		res := result{}

		session := new(SQLDatabase)
		session.On("Exec", sqlQuery).Return(res, errTest)
		query := initQuery(session, new(SQLServer).enquoteValue)
		query.conditions = simpleCondition

		count, err := query.Update(changeDate)

		assert.NotNil(t, err)
		assert.Equal(t, int(res.count), count)
	})

	t.Run("panic", func(t *testing.T) {
		sqlQuery := "UPDATE dbo.players SET " +
			"name = N'Updated Test', rate = 5.7 " +
			"WHERE name = N'Test'"
		changeDate := *base.ZeroRecordData()
		res := result{}

		session := new(SQLDatabase)
		session.On("Exec", sqlQuery).Return(res, errTest)
		query := initQuery(session, new(SQLServer).enquoteValue)
		query.conditions = simpleCondition

		assert.Panics(t, func() {
			_, _ = query.Update(changeDate)
		})
	})
}

func TestSqlQuery_Delete(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		sqlQuery := "DELETE FROM dbo.players WHERE name = N'Test'"
		res := result{rand.Int63n(100)}

		session := new(SQLDatabase)
		session.On("Exec", sqlQuery).Return(res, nil)
		query := initQuery(session, new(SQLServer).enquoteValue)
		query.conditions = simpleCondition

		count, err := query.Delete()

		assert.Nil(t, err)
		assert.Equal(t, int(res.count), count)
	})

	t.Run("failed", func(t *testing.T) {
		sqlQuery := "DELETE FROM dbo.players WHERE name = N'Test'"
		res := result{}

		session := new(SQLDatabase)
		session.On("Exec", sqlQuery).Return(res, errTest)
		query := initQuery(session, new(SQLServer).enquoteValue)
		query.conditions = simpleCondition

		count, err := query.Delete()

		assert.NotNil(t, err)
		assert.Equal(t, int(res.count), count)
	})
}
