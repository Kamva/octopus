package clients

import (
	"fmt"
	"strings"

	"github.com/Kamva/octopus/base"
	"github.com/Kamva/octopus/term"
)

// sqlQuery is a struct containing information about sqlQuery
type sqlQuery struct {
	session    base.SQLDatabase
	table      string
	conditions []base.Condition
	enquoter   base.Enquoter
	sorts      []base.Sort
	limit      int
	offset     int
}

func newSQLQuery(session base.SQLDatabase, table string, conditions []base.Condition, enquoter base.Enquoter) *sqlQuery {
	return &sqlQuery{session: session, table: table, conditions: conditions, enquoter: enquoter}
}

// OrderBy set the order of returning result in following command
func (q *sqlQuery) OrderBy(sorts ...base.Sort) base.QueryBuilder {
	q.sorts = sorts

	return q
}

// Limit set the limit in session that determines how many results should
// be returned in the following fetch command.
func (q *sqlQuery) Limit(n int) base.QueryBuilder {
	q.limit = n

	return q
}

// Skip set the starting offset of the following fetch command
func (q *sqlQuery) Skip(n int) base.QueryBuilder {
	q.offset = n

	return q
}

// Count execute a count command that will return the number records in
// specified destination table. If the query conditions was empty, it
// returns number of all records un destination table.
func (q *sqlQuery) Count() (int, error) {
	data := base.NewRecordData([]string{"count"}, map[string]interface{}{"count": 0})

	rows, err := queryDB(q.session, fmt.Sprintf(
		"SELECT COUNT(*) AS count FROM %s", q.table,
	))

	if err != nil {
		return data.Get("count").(int), err
	}

	err = fetchSingleRecord(rows, data)

	return data.Get("count").(int), err
}

// All returns results that match with sqlQuery conditions in RecordDataSet
// format. If the sqlQuery conditions was empty it will return all records
// in specified destination table or error if anything went wrong.
// It will panic if no destination table was set before call All.
func (q *sqlQuery) All() (base.RecordDataSet, error) {
	whereClause := q.parseWhere()
	optionClause := q.parseOptions()
	var query string

	if whereClause != "" {
		query = strings.TrimRight(fmt.Sprintf(
			"SELECT * FROM %s WHERE %s %s", q.table, whereClause, optionClause,
		), " ")
	} else {
		query = strings.TrimRight(fmt.Sprintf(
			"SELECT * FROM %s %s", q.table, optionClause,
		), " ")
	}

	rows, err := queryDB(q.session, query)
	if err != nil {
		return nil, err
	}

	return fetchResults(rows)
}

// First fetch data of the first record that match with sqlQuery conditions.
func (q *sqlQuery) First() (base.RecordData, error) {
	whereClause := q.parseWhere()
	q.limit = 1
	optionClause := q.parseOptions()

	data := base.ZeroRecordData()
	rows, err := queryDB(q.session, strings.TrimRight(fmt.Sprintf(
		"SELECT * FROM %s WHERE %s %s", q.table, whereClause, optionClause,
	), " "))

	if err != nil {
		return *data, err
	}

	err = fetchSingleRecord(rows, data)

	return *data, err
}

// Update updates records that math with sqlQuery conditions with `data` and
// returns number of affected rows and error if anything went wring. If
// the sqlQuery condition was empty it'll update all records in destination
// table. And panics if no destination table was set before call Update.
func (q *sqlQuery) Update(data base.RecordData) (int, error) {
	if data.Length() == 0 {
		panic("change data could not be empty")
	}

	setClause := q.parseChanges(data)
	whereClause := q.parseWhere()

	res, err := q.session.Exec(fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s", q.table, setClause, whereClause,
	))
	rowsAffected, _ := res.RowsAffected()

	return int(rowsAffected), err
}

// Delete removes every records in destination table that match with condition
// sqlQuery and returns number of affected rows and error if anything went wrong.
// It will removes all records inside destination table if no condition sqlQuery
// was set and panics if the destination table is not set before call Delete.
func (q *sqlQuery) Delete() (int, error) {
	whereClause := q.parseWhere()

	res, err := q.session.Exec(fmt.Sprintf(
		"DELETE FROM %s WHERE %s", q.table, whereClause,
	))
	rowsAffected, _ := res.RowsAffected()

	return int(rowsAffected), err
}

func (q *sqlQuery) parseWhere() string {
	clauses := make([]string, 0, len(q.conditions))
	for _, condition := range q.conditions {
		switch condition.(type) {
		case term.Equal:
			clauses = append(clauses, fmt.Sprintf(
				"%s = %s", condition.GetField(), q.enquoter(condition.GetValue()),
			))
		case term.NotEqual:
			clauses = append(clauses, fmt.Sprintf(
				"%s != %s", condition.GetField(), q.enquoter(condition.GetValue()),
			))
		case term.GreaterThan:
			clauses = append(clauses, fmt.Sprintf(
				"%s > %s", condition.GetField(), q.enquoter(condition.GetValue()),
			))
		case term.GreaterThanEqual:
			clauses = append(clauses, fmt.Sprintf(
				"%s >= %s", condition.GetField(), q.enquoter(condition.GetValue()),
			))
		case term.LessThan:
			clauses = append(clauses, fmt.Sprintf(
				"%s < %s", condition.GetField(), q.enquoter(condition.GetValue()),
			))
		case term.LessThanEqual:
			clauses = append(clauses, fmt.Sprintf(
				"%s <= %s", condition.GetField(), q.enquoter(condition.GetValue()),
			))
		case term.IsNull:
			clauses = append(clauses, fmt.Sprintf(
				"%s IS NULL", condition.GetField(),
			))
		case term.NotNull:
			clauses = append(clauses, fmt.Sprintf(
				"%s IS NOT NULL", condition.GetField(),
			))
		case term.In:
			values := condition.GetValue().([]interface{})
			valueStrings := make([]string, 0, len(values))
			for _, value := range values {
				valueStrings = append(valueStrings, q.enquoter(value))
			}
			clauses = append(clauses, fmt.Sprintf(
				"%s IN (%s)", condition.GetField(), strings.Join(valueStrings, ", "),
			))
		}
	}

	return strings.Join(clauses, " AND ")
}

func (q *sqlQuery) parseOptions() (query string) {
	if q.limit > 0 {
		query += fmt.Sprintf("LIMIT %v ", q.limit)
	}

	if q.offset > 0 {
		query += fmt.Sprintf("OFFSET %v ", q.offset)
	}

	sorts := make([]string, 0, len(q.sorts))
	for _, sort := range q.sorts {
		var order string
		if sort.Descending {
			order = "DESC"
		} else {
			order = "ASC"
		}
		sorts = append(sorts, fmt.Sprintf("%s %s", sort.Column, order))
	}

	if len(sorts) > 0 {
		query += fmt.Sprintf("ORDER BY %s", strings.Join(sorts, ", "))
	}

	return strings.TrimRight(query, " ")
}

func (q *sqlQuery) parseChanges(data base.RecordData) interface{} {
	changeSet := make([]string, 0)
	for _, column := range data.GetColumns() {
		changeSet = append(changeSet, fmt.Sprintf(
			"%s = %s", column, q.enquoter(data.Get(column))),
		)
	}

	return strings.Join(changeSet, ", ")
}
