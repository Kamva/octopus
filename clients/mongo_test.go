package clients

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/kamva/octopus/base"
	. "github.com/kamva/octopus/clients/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ----------------------
//    Helper functions
// ----------------------

var conditionsMap = bson.M{
	"age":          19,
	"team":         bson.M{"$ne": "Manchester City"},
	"rate":         bson.M{"$gt": 8.5},
	"score":        bson.M{"$gte": 10},
	"yellow_cards": bson.M{"$lt": 2},
	"red_cards":    bson.M{"$lte": 1},
	"grade":        bson.M{"$in": []interface{}{"A", "B"}},
	"banned_date":  bson.M{"$eq": nil},
	"trophies":     bson.M{"$ne": nil},
}

type queryByIDFunc func(c *MongoDB, collection string, id interface{}) base.MongoQuery

var dialMock = func(url string) (base.MongoSession, error) {
	if url == "invalid url" {
		return nil, errTest
	}

	return new(MongoSession), nil
}

var getQueryByIDMock = func(c *MongoDB, collection string, id interface{}, query *MongoQuery) queryByIDFunc {
	return func(c *MongoDB, collection string, id interface{}) base.MongoQuery {
		return query
	}
}

func initMongo(session base.MongoSession, collection base.MongoCollection) *MongoDB {
	return &MongoDB{session: session, dbName: "test", collection: collection}
}

// ----------------
//    Unit Tests
// ----------------

func TestNewMongoDB(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		original := dial
		defer func() { dial = original }()
		dial = dialMock

		client := NewMongoDB("localhost:27017", "test")

		mongo := client.(*MongoDB)

		assert.Equal(t, "test", mongo.dbName)
		assert.NotNil(t, mongo.session)
	})

	t.Run("fail", func(t *testing.T) {
		original := dial
		defer func() { dial = original }()
		dial = dialMock

		assert.Panics(t, func() {
			NewMongoDB("invalid url", "test")
		})
	})
}

func TestMongoDB_CreateTable(t *testing.T) {
	t.Run("mgoCollection", func(t *testing.T) {
		collectionInfo := &mgo.CollectionInfo{DisableIdIndex: true}
		info := base.CollectionInfo{
			Info: collectionInfo,
		}
		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("Create", collectionInfo).Return(nil)

		client := initMongo(session, collection)
		err := client.CreateTable("users", info)

		assert.Nil(t, err)
	})

	t.Run("normalCollection", func(t *testing.T) {
		info := base.TableStructure{}
		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("Create", mock.AnythingOfType("*mgo.CollectionInfo")).
			Return(nil)

		client := initMongo(session, collection)
		err := client.CreateTable("users", info)

		assert.Nil(t, err)
	})

	t.Run("error", func(t *testing.T) {
		collectionInfo := &mgo.CollectionInfo{DisableIdIndex: true}
		info := base.CollectionInfo{
			Info: collectionInfo,
		}
		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("Create", collectionInfo).Return(errTest)

		client := initMongo(session, collection)
		err := client.CreateTable("users", info)

		assert.NotNil(t, err)
	})
}

func TestMongoDB_EnsureIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		index := base.Index{Columns: []string{"name"}, Unique: true}
		mIndex := mgo.Index{Key: []string{"name"}, Unique: true}

		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("EnsureIndex", mIndex).Return(nil)

		client := initMongo(session, collection)

		err := client.EnsureIndex("users", index)

		assert.Nil(t, err)
	})

	t.Run("error", func(t *testing.T) {
		index := base.Index{Columns: []string{"name"}, Unique: true}
		mIndex := mgo.Index{Key: []string{"name"}, Unique: true}

		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("EnsureIndex", mIndex).Return(errTest)

		client := initMongo(session, collection)

		err := client.EnsureIndex("users", index)

		assert.NotNil(t, err)
	})
}

func TestMongoDB_Insert(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		data := base.NewRecordData(
			[]string{"name", "age", "status"},
			base.RecordMap{"name": "Test", "age": 1, "status": true},
		)

		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("Insert", data.GetMap()).Return(nil)

		client := initMongo(session, collection)

		err := client.Insert("users", data)

		assert.Nil(t, err)

		assert.NotNil(t, data.Get("_id"))
	})

	t.Run("error", func(t *testing.T) {
		data := base.NewRecordData(
			[]string{"name", "age", "status"},
			base.RecordMap{"name": "Test", "age": 1, "status": true},
		)

		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("Insert", data.GetMap()).Return(errTest)

		client := initMongo(session, collection)

		err := client.Insert("users", data)

		assert.NotNil(t, err)
	})
}

func TestMongoDB_FindByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		original := queryByID
		defer func() { queryByID = original }()

		id := bson.NewObjectId()
		session := new(MongoSession)
		collection := new(MongoCollection)
		query := new(MongoQuery)
		var data = base.ZeroRecordData()
		query.On("One", data.GetMap()).Return(nil).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*base.RecordMap)
			(*arg)["_id"] = id
			(*arg)["name"] = "Test"
			(*arg)["age"] = 1
			(*arg)["status"] = false
		})

		client := initMongo(session, collection)
		queryByID = getQueryByIDMock(client, "users", id, query)

		res, err := client.FindByID("users", id)

		assert.Nil(t, err)
		assert.IsType(t, base.RecordData{}, res)
		assert.Equal(t, id, res.Get("_id"))
		assert.Equal(t, "Test", res.Get("name"))
		assert.Equal(t, 1, res.Get("age"))
		assert.Equal(t, false, res.Get("status"))
	})

	t.Run("error", func(t *testing.T) {
		original := queryByID
		defer func() { queryByID = original }()

		id := bson.NewObjectId()
		session := new(MongoSession)
		collection := new(MongoCollection)
		query := new(MongoQuery)
		var data = base.ZeroRecordData()
		query.On("One", data.GetMap()).Return(errTest)

		client := initMongo(session, collection)
		queryByID = getQueryByIDMock(client, "users", id, query)

		res, err := client.FindByID("users", id)

		assert.NotNil(t, err)
		assert.IsType(t, base.RecordData{}, res)
		assert.Nil(t, res.Get("_id"))
		assert.Nil(t, res.Get("name"))
		assert.Nil(t, res.Get("age"))
		assert.Nil(t, res.Get("status"))
	})
}

func TestMongoDB_UpdateByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		data := base.NewRecordData(
			[]string{"name", "age", "status"},
			base.RecordMap{"name": "Test Updated", "age": 2, "status": false},
		)

		id := bson.NewObjectId()
		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("UpdateId", id, data.GetMap()).Return(nil)

		client := initMongo(session, collection)
		err := client.UpdateByID("users", id, *data)

		assert.Nil(t, err)
	})

	t.Run("error", func(t *testing.T) {
		data := base.NewRecordData(
			[]string{"name", "age", "status"},
			base.RecordMap{"name": "Test Updated", "age": 2, "status": false},
		)

		id := bson.NewObjectId()
		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("UpdateId", id, data.GetMap()).Return(errTest)

		client := initMongo(session, collection)
		err := client.UpdateByID("users", id, *data)

		assert.NotNil(t, err)
	})
}

func TestMongoDB_DeleteByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		id := bson.NewObjectId()
		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("RemoveId", id).Return(nil)

		client := initMongo(session, collection)
		err := client.DeleteByID("users", id)

		assert.Nil(t, err)
	})

	t.Run("error", func(t *testing.T) {
		id := bson.NewObjectId()
		session := new(MongoSession)
		collection := new(MongoCollection)
		collection.On("RemoveId", id).Return(errTest)

		client := initMongo(session, collection)
		err := client.DeleteByID("users", id)

		assert.NotNil(t, err)
	})
}

func TestMongoDB_Query(t *testing.T) {
	session := new(MongoSession)
	collection := new(MongoCollection)
	query := new(mgo.Query)
	collection.On("Find", conditionsMap).Return(query)

	client := initMongo(session, collection)

	q := client.Query("users", conditions...)

	assert.NotNil(t, q)
	assert.IsType(t, (*mongoQuery)(nil), q)
}

func TestMongoDB_Close(t *testing.T) {
	session := new(MongoSession)
	collection := new(MongoCollection)

	session.On("Close").Return()

	client := initMongo(session, collection)

	client.Close()

	assert.Nil(t, client.collection)
	assert.Nil(t, client.session)
	assert.Equal(t, "", client.dbName)
}

func TestMongoDB_convertID(t *testing.T) {
	t.Run("objectId", func(t *testing.T) {
		session := new(MongoSession)
		client := initMongo(session, new(MongoCollection))

		id := bson.NewObjectId()
		ret := client.convertID(id)

		assert.Equal(t, id, ret)
	})

	t.Run("string", func(t *testing.T) {
		session := new(MongoSession)
		client := initMongo(session, new(MongoCollection))

		id := bson.NewObjectId().Hex()
		ret := client.convertID(id)

		assert.Equal(t, id, ret.Hex())
		assert.Equal(t, bson.ObjectIdHex(id), ret)
	})

	t.Run("invalid", func(t *testing.T) {
		session := new(MongoSession)
		client := initMongo(session, new(MongoCollection))

		assert.Panics(t, func() {
			client.convertID(10)
		})
	})
}
