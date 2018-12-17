package clients

import (
	"fmt"

	"github.com/globalsign/mgo/bson"
	"github.com/kamva/octopus/base"
)

// MongoQuery is a struct containing mgo.Query object
type mongoQuery struct {
	query      base.MongoQuery
	collection base.MongoCollection
	queryMap   bson.M
}

// OrderBy set the order of returning result in following command
func (q *mongoQuery) OrderBy(sorts ...base.Sort) base.Query {
	for _, sort := range sorts {
		if sort.Descending {
			q.query.Sort(fmt.Sprintf("-%s", sort.Column))
		} else {
			q.query.Sort(sort.Column)
		}
	}

	return q
}

// Limit set the limit that determines how many results should be
// returned in the following fetch command.
func (q *mongoQuery) Limit(n int) base.Query {
	q.query.Limit(n)

	return q
}

// Skip set the starting offset of the following fetch command
func (q *mongoQuery) Skip(n int) base.Query {
	q.query.Skip(n)

	return q
}

// Count execute a count command that will return the number records in
// specified destination table. If the query conditions was empty, it
// returns number of all records un destination table.
func (q *mongoQuery) Count() (int, error) {
	return q.query.Count()
}

// First fetch data of the first record that match with query conditions.
func (q *mongoQuery) First() (base.RecordData, error) {
	data := base.ZeroRecordData()
	doc := make(base.RecordMap)

	err := q.query.One(&doc)

	// if there's no error we fill RecordData struct
	// otherwise an empty RecordData and err will return
	if err == nil {
		for key, value := range doc {
			data.Set(key, value)
		}
	}

	return *data, err
}

// All returns results that match with query conditions in RecordDataSet
// format. If the query conditions was empty it will return all records
// in specified destination table or error if anything went wrong.
func (q *mongoQuery) All() (base.RecordDataSet, error) {
	resultSet := make(base.RecordDataSet, 0)
	items := make([]base.RecordMap, 0)
	err := q.query.All(&items)

	// if there's no error we fill resultSet
	// otherwise an empty resultSet and err will return
	if err == nil {
		data := *base.ZeroRecordData()
		for _, item := range items {
			for key, value := range item {
				data.Set(key, value)
			}

			resultSet = append(resultSet, data)
			data.Zero()
		}
	}

	return resultSet, err
}

// Update updates records that math with query conditions with `data` and
// returns number of affected rows and error if anything went wring. If
// the query condition was empty it'll update all records in destination
// table.
func (q *mongoQuery) Update(data base.RecordData) (int, error) {
	set := bson.M{}
	for column, value := range *data.GetMap() {
		set[column] = value
	}
	update := bson.M{"$set": set}

	changeInfo, err := q.collection.UpdateAll(q.queryMap, update)

	return changeInfo.Updated, err
}

// Delete removes every records in destination table that match with condition
// query and returns number of affected rows and error if anything went wrong.
// It will removes all records inside destination table if no condition query
// was set.
func (q *mongoQuery) Delete() (int, error) {
	changeInfo, err := q.collection.RemoveAll(q.queryMap)

	return changeInfo.Removed, err
}

func newMongoQuery(query base.MongoQuery, collection base.MongoCollection, queryMap bson.M) *mongoQuery {
	return &mongoQuery{query: query}
}
