package clients

import (
	"testing"

	"github.com/Kamva/octopus/base"
	. "github.com/Kamva/octopus/clients/internal"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ----------------------
//    Helper functions
// ----------------------

func initMongoBuilder(query *MongoQuery) *mongoQuery {
	return &mongoQuery{query: query}
}

func initMongoBuilderWithCollection(query *MongoQuery, collection base.MongoCollection) *mongoQuery {
	return &mongoQuery{query: query, collection: collection, queryMap: conditionsMap}
}

// ----------------
//    Unit Tests
// ----------------

func TestMongoQuery_OrderBy(t *testing.T) {
	t.Run("ascending", func(t *testing.T) {
		query := new(MongoQuery)
		query.On("Sort", "name").Return(new(mgo.Query))
		sort := base.Sort{Column: "name"}
		q := initMongoBuilder(query).OrderBy(sort)

		assert.IsType(t, new(mongoQuery), q)
	})

	t.Run("descending", func(t *testing.T) {
		query := new(MongoQuery)
		query.On("Sort", "-name").Return(new(mgo.Query))
		sort := base.Sort{Column: "name", Descending: true}
		q := initMongoBuilder(query).OrderBy(sort)

		assert.IsType(t, new(mongoQuery), q)
	})

	t.Run("multiple", func(t *testing.T) {
		query := new(MongoQuery)
		rQuery := new(mgo.Query)
		query.On("Sort", "name").Return(rQuery)
		query.On("Sort", "-age").Return(rQuery)
		sort1 := base.Sort{Column: "name"}
		sort2 := base.Sort{Column: "age", Descending: true}
		q := initMongoBuilder(query).OrderBy(sort1, sort2)

		assert.IsType(t, new(mongoQuery), q)
	})
}

func TestMongoBuilder_Limit(t *testing.T) {
	query := new(MongoQuery)
	rQuery := new(mgo.Query)
	query.On("Limit", 5).Return(rQuery)
	q := initMongoBuilder(query).Limit(5)

	assert.IsType(t, new(mongoQuery), q)
}

func TestMongoBuilder_Skip(t *testing.T) {
	query := new(MongoQuery)
	rQuery := new(mgo.Query)
	query.On("Skip", 5).Return(rQuery)
	q := initMongoBuilder(query).Skip(5)

	assert.IsType(t, new(mongoQuery), q)
}

func TestMongoBuilder_Count(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		query := new(MongoQuery)
		resCount := 8
		query.On("Count").Return(resCount, nil)
		count, err := initMongoBuilder(query).Count()

		assert.Nil(t, err)
		assert.Equal(t, resCount, count)
	})

	t.Run("notFoundOrErr", func(t *testing.T) {
		query := new(MongoQuery)
		query.On("Count").Return(0, errTest)
		count, err := initMongoBuilder(query).Count()

		assert.NotNil(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestMongoBuilder_First(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		query := new(MongoQuery)
		var data = base.ZeroRecordData()
		id := bson.NewObjectId()
		query.On("One", data.GetMap()).Return(nil).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*base.RecordMap)
			(*arg)["_id"] = id
			(*arg)["name"] = "Test"
			(*arg)["age"] = 1
			(*arg)["status"] = false
		})
		res, err := initMongoBuilder(query).First()

		assert.Nil(t, err)
		assert.IsType(t, base.RecordData{}, res)
		assert.Equal(t, id, res.Get("_id"))
		assert.Equal(t, "Test", res.Get("name"))
		assert.Equal(t, 1, res.Get("age"))
		assert.Equal(t, false, res.Get("status"))
	})

	t.Run("notFound", func(t *testing.T) {
		query := new(MongoQuery)
		var data = base.ZeroRecordData()
		query.On("One", data.GetMap()).Return(errTest)
		res, err := initMongoBuilder(query).First()

		assert.NotNil(t, err)
		assert.IsType(t, base.RecordData{}, res)
		assert.Nil(t, res.Get("_id"))
		assert.Nil(t, res.Get("name"))
		assert.Nil(t, res.Get("age"))
		assert.Nil(t, res.Get("status"))
	})
}

func TestMongoBuilder_All(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		query := new(MongoQuery)
		items := make([]base.RecordMap, 0)
		resultCount := 8
		query.On("All", &items).Return(nil).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*[]base.RecordMap)
			for i := 0; i < resultCount; i++ {
				data := make(base.RecordMap)
				data["id"] = bson.NewObjectId()
				data["name"] = "Test"
				data["age"] = 1
				data["status"] = false
				*arg = append(*arg, data)
			}
		})
		res, err := initMongoBuilder(query).All()

		assert.Nil(t, err)
		assert.Equal(t, resultCount, len(res))
	})

	t.Run("notFound", func(t *testing.T) {
		query := new(MongoQuery)
		items := make([]base.RecordMap, 0)
		query.On("All", &items).Return(errTest)
		res, err := initMongoBuilder(query).All()

		assert.NotNil(t, err)
		assert.Equal(t, 0, len(res))
	})
}

func TestMongoBuilder_Update(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		query := new(MongoQuery)
		collection := new(MongoCollection)
		changes := *base.NewRecordData(
			[]string{"name", "status"},
			base.RecordMap{"name": "Updated Test", "status": false},
		)
		update := bson.M{"$set": bson.M{"name": "Updated Test", "status": false}}

		updatedRows := 10
		changeInfo := &mgo.ChangeInfo{Updated: updatedRows}

		collection.On("UpdateAll", conditionsMap, update).Return(changeInfo, nil)
		res, err := initMongoBuilderWithCollection(query, collection).Update(changes)

		assert.Nil(t, err)
		assert.Equal(t, updatedRows, res)
	})

	t.Run("notFound", func(t *testing.T) {
		query := new(MongoQuery)
		collection := new(MongoCollection)
		changes := *base.NewRecordData(
			[]string{"name", "status"},
			base.RecordMap{"name": "Updated Test", "status": false},
		)
		update := bson.M{"$set": bson.M{"name": "Updated Test", "status": false}}

		collection.On("UpdateAll", conditionsMap, update).Return(&mgo.ChangeInfo{}, errTest)
		res, err := initMongoBuilderWithCollection(query, collection).Update(changes)

		assert.NotNil(t, err)
		assert.Equal(t, 0, res)
	})
}

func TestMongoBuilder_Delete(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		query := new(MongoQuery)
		collection := new(MongoCollection)

		removedRows := 10
		changeInfo := &mgo.ChangeInfo{Removed: removedRows}

		collection.On("RemoveAll", conditionsMap).Return(changeInfo, nil)
		res, err := initMongoBuilderWithCollection(query, collection).Delete()

		assert.Nil(t, err)
		assert.Equal(t, removedRows, res)
	})

	t.Run("notFound", func(t *testing.T) {
		query := new(MongoQuery)
		collection := new(MongoCollection)

		collection.On("RemoveAll", conditionsMap).Return(&mgo.ChangeInfo{}, errTest)
		res, err := initMongoBuilderWithCollection(query, collection).Delete()

		assert.NotNil(t, err)
		assert.Equal(t, 0, res)
	})
}
