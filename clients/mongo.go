package clients

import (
	"github.com/Kamva/octopus/base"
	"github.com/Kamva/octopus/term"
	"github.com/Kamva/shark"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// MongoDB is a client for the MongoDB
type MongoDB struct {
	session    base.MongoSession
	dbName     string
	collection base.MongoCollection
}

// CreateTable creates a `collectionName` collection. Since MongoDB is a
// schema-less database, creating a collection is not necessary before
// interacting with it and MongoDB creates collections automatically.
// Here you can create a collection with a special characteristics.
// You could use `mgo.CollectionInfo` tp specify these characteristics,
// and wrap it around `base.CollectionInfo` and pass it to this method.
// It is better use EnsureIndex for creating ordinary collection.
func (c *MongoDB) CreateTable(collectionName string, info base.TableInfo) error {
	if collectionInfo, ok := info.GetInfo().(*mgo.CollectionInfo); ok {
		return c.GetCollection(collectionName).Create(collectionInfo)
	}

	collectionInfo := &mgo.CollectionInfo{}
	return c.GetCollection(collectionName).Create(collectionInfo)
}

// EnsureIndex ensures that given index is exists on given collection.
// If not, tries to create an index with given condition on given collection.
// EnsureIndex also creates the collection if it is not exists on DB.
func (c *MongoDB) EnsureIndex(collectionName string, index base.Index) error {
	return c.GetCollection(collectionName).EnsureIndex(mgo.Index{
		Key:    index.Columns,
		Unique: index.Unique,
	})
}

// Insert tries to insert `data` into `collectionName` and returns error if
// anything went wrong. `data` should pass by reference to have exact
// data on `collectionName`, otherwise updated record data isn't accessible.
func (c *MongoDB) Insert(collectionName string, data *base.RecordData) error {
	data.Set("_id", bson.NewObjectId())
	err := c.GetCollection(collectionName).Insert(data.GetMap())

	return err
}

// FindByID searches through `collectionName` documents to find a doc that its
// ID match with `id` and returns it alongside any possible error.
func (c *MongoDB) FindByID(collectionName string, id interface{}) (base.RecordData, error) {
	data := base.ZeroRecordData()
	doc := make(base.RecordMap)

	err := queryByID(c, collectionName, id).One(&doc)

	for key, value := range doc {
		data.Set(key, value)
	}

	return *data, err
}

// UpdateByID finds a document in `collectionName` that its ID match with `id`,
// and updates it with data. It will return error if anything went wrong.
func (c *MongoDB) UpdateByID(collectionName string, id interface{}, data base.RecordData) error {
	return c.GetCollection(collectionName).UpdateId(id, data.GetMap())
}

// DeleteByID finds a document in `collectionName` that its ID match with `id`,
// and remove it entirely. It will return error if anything went wrong.
func (c *MongoDB) DeleteByID(collectionName string, id interface{}) error {
	return c.GetCollection(collectionName).RemoveId(id)
}

// Query generates and returns query object for further operations
func (c *MongoDB) Query(collectionName string, conditions ...base.Condition) base.QueryBuilder {
	queryMap := c.parseConditions(conditions...)
	query := queryMongoDB(c, collectionName, queryMap)

	return newMongoQuery(query, c.GetCollection(collectionName), queryMap)
}

// Close disconnect client from database and release the taken memory
func (c *MongoDB) Close() {
	c.session.Close()
	c.session = nil
	c.collection = nil
	c.dbName = ""
}

// GetCollection return collection instance with given name
func (c *MongoDB) GetCollection(collection string) base.MongoCollection {
	if c.collection == nil {
		c.collection = c.session.DB(c.dbName).C(collection)
	}

	return c.collection
}

// convert given interface id to objectId
func (c *MongoDB) convertID(id interface{}) bson.ObjectId {
	switch id.(type) {
	case string:
		return bson.ObjectIdHex(id.(string))
	case bson.ObjectId:
		return id.(bson.ObjectId)
	}

	panic("Invalid ID for mongodb document.")
}

// Parse conditions query into map of mongo query (bson.M)
func (c *MongoDB) parseConditions(conditions ...base.Condition) bson.M {
	queryMap := make(bson.M)
	for _, condition := range conditions {
		switch condition.(type) {
		case term.Equal:
			queryMap[condition.GetField()] = condition.GetValue()
			break
		case term.GreaterThan:
			queryMap[condition.GetField()] = bson.M{
				"$gt": condition.GetValue(),
			}
			break
		case term.GreaterThanEqual:
			queryMap[condition.GetField()] = bson.M{
				"$gte": condition.GetValue(),
			}
			break
		case term.In:
			queryMap[condition.GetField()] = bson.M{
				"$in": condition.GetValue(),
			}
			break
		case term.IsNull:
			queryMap[condition.GetField()] = bson.M{
				"$eq": condition.GetValue(),
			}
			break
		case term.LessThan:
			queryMap[condition.GetField()] = bson.M{
				"$lt": condition.GetValue(),
			}
			break
		case term.LessThanEqual:
			queryMap[condition.GetField()] = bson.M{
				"$lte": condition.GetValue(),
			}
			break
		case term.NotEqual:
			queryMap[condition.GetField()] = bson.M{
				"$ne": condition.GetValue(),
			}
			break
		case term.NotNull:
			queryMap[condition.GetField()] = bson.M{
				"$ne": condition.GetValue(),
			}
			break
		}
	}

	return queryMap
}

// NewMongoDB instantiates and returns a ne MongoDB session object
func NewMongoDB(url string, dbName string) base.Client {
	session, err := dial(url)
	shark.PanicIfError(err)

	return &MongoDB{
		session: session,
		dbName:  dbName,
	}
}

// These functions will make mocking mgo.Dial function and mgo.QueryBuilder easier
var dial = func(url string) (base.MongoSession, error) {
	return mgo.Dial(url)
}

var queryByID = func(c *MongoDB, collection string, id interface{}) base.MongoQuery {
	return c.GetCollection(collection).FindId(c.convertID(id))
}

var queryMongoDB = func(c *MongoDB, collection string, conditions bson.M) base.MongoQuery {
	return c.GetCollection(collection).Find(conditions)
}
