package clients

import (
	"fmt"
	"strings"

	"github.com/kamva/octopus/base"
	"github.com/kataras/iris/core/errors"
)

// fetchSingleRecord Fetch a single result from rows and set into record data
func fetchSingleRecord(rows base.SQLRows, data *base.RecordData) error {
	if rows.Next() {
		// Get list of result columns
		cols, _ := rows.Columns()

		// get column pointers variable
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// scan the result into column pointers
		err := rows.Scan(columnPointers...)

		// set retrieved data from db to record data
		for i, colName := range cols {
			data.Set(colName, columns[i])
		}

		return err
	}

	data.Zero()

	return errors.New("no result found")
}

func fetchResults(rows base.SQLRows) (base.RecordDataSet, error) {
	// Get list of result columns
	cols, _ := rows.Columns()

	resultSet := make(base.RecordDataSet, 0)
	data := *base.ZeroRecordData()

	for rows.Next() {
		// get column pointers variable
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		err := rows.Scan(columnPointers...)

		if err != nil {
			return nil, err
		}

		// set retrieved data from db to record data
		for i, colName := range cols {
			data.Set(colName, columns[i])
		}
		resultSet = append(resultSet, data)
		data.Zero()
	}

	return resultSet, nil
}

func prepareUpdate(data base.RecordData, enquoter base.Enquoter) string {
	updateParts := make([]string, 0, data.Length())
	for _, column := range data.GetColumns() {
		updateParts = append(updateParts, fmt.Sprintf("%s = %s", column, enquoter(data.Get(column))))
	}

	return strings.Join(updateParts, ", ")
}

// queryDB executes given sqlQuery string and returns result rows and error
// This is separated as a variable to mocked easily
var queryDB = func(db base.SQLDatabase, query string) (base.SQLRows, error) {
	return db.Query(query)
}
