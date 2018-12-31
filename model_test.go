package octopus

import (
	"errors"
	"testing"

	"github.com/Kamva/octopus/base"
	. "github.com/Kamva/octopus/internal"
	"github.com/Kamva/octopus/term"
	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ----------------------
//    Helper functions
// ----------------------

var newMongoMock = func(url string, dbName string) base.Client {
	mock := new(Client)
	mock.On("Close").Return()
	return mock
}

var newSQLClientMock = func(url string) base.Client {
	mock := new(Client)
	mock.On("Close").Return()
	return mock
}

var errTest = errors.New("something went wrong")

func makeModel(s base.Scheme, c base.DBConfig, cn ...Configurator) Model {
	model := Model{}
	model.Initiate(s, c, cn...)

	return model
}

type scheme struct {
	Scheme
}

func (s scheme) GetID() interface{} {
	panic("this should not be run")
}

type User struct {
	scheme
	ID     bson.ObjectId
	Name   string
	Age    int
	Status bool `sql:"column:available"`
}

func (u User) GetID() interface{} {
	return u.ID
}

type Profile struct {
	scheme
	ID     int
	Name   string
	Age    int
	Status bool
	Rate   float32
	Score  uint
	Worth  uint64
}

func (p Profile) GetSchema() string {
	return "acc"
}

type pg struct {
	scheme
	ID          int
	Bool        bool    `sql:"notnull"`
	SmallSerial int8    `sql:"ai"`
	SmallInt    int16   `sql:"check:small_int > 0"`
	Int         int32   `sql:"default:0"`
	BigSerial   int64   `sql:"ai"`
	BigInt      uint    `sql:"unique"`
	Real        float32 `sql:"null"`
	Float       float64 `sql:"pk"`
	Decimal     uint64
	Array       []int
	JSON        map[string]interface{}
	Text        string      `sql:"column:string"`
	Varchar     string      `sql:"column:string2;type:varchar(50)"`
	Ignore      interface{} `sql:"ignore"`
}

var pgStructure = base.TableStructure{
	{Name: "id", Type: "SERIAL", Options: "PRIMARY KEY"},
	{Name: "bool", Type: "BOOLEAN", Options: "NOT NULL"},
	{Name: "small_serial", Type: "SMALLSERIAL"},
	{Name: "small_int", Type: "SMALLINT", Options: "CHECK (small_int > 0)"},
	{Name: "int", Type: "INT", Options: "DEFAULT 0"},
	{Name: "big_serial", Type: "BIGSERIAL"},
	{Name: "big_int", Type: "BIGINT", Options: "UNIQUE"},
	{Name: "real", Type: "REAL", Options: "NULL"},
	{Name: "float", Type: "FLOAT8", Options: "PRIMARY KEY"},
	{Name: "decimal", Type: "DECIMAL"},
	{Name: "array", Type: "INT[]"},
	{Name: "json", Type: "JSON"},
	{Name: "string", Type: "TEXT"},
	{Name: "string2", Type: "varchar(50)"},
}

type mssql struct {
	scheme
	ID       int
	TinyInt  uint8   `sql:"id"`
	SmallInt int8    `sql:"id:(2,5)"`
	Bit      bool    `sql:"pk"`
	BigInt   int64   `sql:"pk;cluster"`
	Real     float32 `sql:"pk;noncluster;default:0"`
	Float    float64 `sql:"unique;check:(float > 10)"`
	Decimal  uint64  `sql:"unique;cluster;null"`
	Text     string  `sql:"unique;noncluster;notnull"`
}

var mssqlStructure = base.TableStructure{
	{Name: "id", Type: "INT", Options: "IDENTITY PRIMARY KEY"},
	{Name: "tiny_int", Type: "TINYINT", Options: "IDENTITY"},
	{Name: "small_int", Type: "SMALLINT", Options: "IDENTITY(2,5)"},
	{Name: "bit", Type: "BIT", Options: "PRIMARY KEY"},
	{Name: "big_int", Type: "BIGINT", Options: "PRIMARY KEY CLUSTERED"},
	{Name: "real", Type: "REAL", Options: "PRIMARY KEY NONCLUSTERED DEFAULT 0"},
	{Name: "float", Type: "FLOAT", Options: "UNIQUE CHECK (float > 10)"},
	{Name: "decimal", Type: "DECIMAL", Options: "NULL UNIQUE CLUSTERED"},
	{Name: "text", Type: "NVARCHAR(MAX)", Options: "NOT NULL UNIQUE NONCLUSTERED"},
}

type pgInvalid struct {
	scheme
	Func func()
}

type mssqlInvalid struct {
	scheme
	Array []int
}

type inner struct {
	Field1 int
	Field2 string
}

type Special struct {
	scheme
	Map          base.JSONMap
	Struct       inner
	SliceInt     []int
	SliceInt8    []int8
	SliceInt16   []int16
	SliceInt32   []int32
	SliceInt64   []int64
	SliceUint    []uint
	SliceUint8   []uint8
	SliceUint16  []uint16
	SliceUint32  []uint32
	SliceUint64  []uint64
	SliceFloat32 []float32
	SliceFloat64 []float64
	SliceString  []string
	SliceJSON    []base.JSONMap `sql:"column:slice_json"`
	SliceStruct  []inner
	SliceBool    []bool
}

type Invalid struct {
	scheme
	Wrong func() int
}

type Invalid2 struct {
	scheme
	Wrong []func() int
}

var conditions = []base.Condition{
	term.Equal{Field: "age", Value: 19},
	term.NotEqual{Field: "status", Value: false},
}

// ----------------
//    Unit Tests
// ----------------

func TestModel_Initiate(t *testing.T) {
	u := &User{}
	p := &Profile{}
	config := base.DBConfig{Driver: base.Mongo}

	t.Run("withoutPrefixWithoutConfigurator", func(t *testing.T) {
		model := makeModel(u, config)

		assert.Equal(t, "users", model.tableName)
		assert.Equal(t, u, model.scheme)
		assert.Equal(t, config, model.config)
	})

	t.Run("withoutPrefixWithConfigurator", func(t *testing.T) {
		model := makeModel(u, config, func(model *Model) {
			model.tableName = "custom"
		})

		assert.Equal(t, "custom", model.tableName)
		assert.Equal(t, u, model.scheme)
		assert.Equal(t, config, model.config)
	})

	t.Run("withPrefixWithoutConfigurator", func(t *testing.T) {
		config := base.DBConfig{Driver: base.Mongo}
		config.Prefix = "test"
		model := makeModel(u, config)

		assert.Equal(t, "test_users", model.tableName)
		assert.Equal(t, u, model.scheme)
		assert.Equal(t, config, model.config)
	})

	t.Run("withPrefixWithConfigurator", func(t *testing.T) {
		config.Prefix = "test"
		model := makeModel(u, config, func(model *Model) {
			model.tableName = "custom"
		})

		assert.Equal(t, "test_custom", model.tableName)
		assert.Equal(t, u, model.scheme)
		assert.Equal(t, config, model.config)
	})

	t.Run("withoutPrefixWithoutConfiguratorSqlSrvDriver", func(t *testing.T) {
		config := base.DBConfig{Driver: base.MSSQL}
		model := makeModel(u, config)

		assert.Equal(t, "dbo.users", model.tableName)
		assert.Equal(t, u, model.scheme)
		assert.Equal(t, config, model.config)
	})

	t.Run("withMsSchemeForSqlDriver", func(t *testing.T) {
		config := base.DBConfig{Driver: base.MSSQL}
		model := makeModel(p, config)

		assert.Equal(t, "acc.profiles", model.tableName)
		assert.Equal(t, p, model.scheme)
		assert.Equal(t, config, model.config)
	})

	t.Run("ptrPanic", func(t *testing.T) {
		assert.Panics(t, func() {
			_ = makeModel(*u, config)
		})
	})
}

func TestModel_EnsureIndex(t *testing.T) {
	t.Run("mongo", func(t *testing.T) {
		config := base.DBConfig{Driver: base.Mongo}
		model := makeModel(&User{}, config)

		t.Run("singleIndex", func(t *testing.T) {
			index := base.Index{Columns: []string{"age"}}

			client := new(Client)
			client.On("Close").Return()
			client.On("EnsureIndex", "users", index).Return(nil)
			model.client = client

			assert.NotPanics(t, func() {
				model.EnsureIndex(index)
			})
		})

		t.Run("multipleIndex", func(t *testing.T) {
			index1 := base.Index{Columns: []string{"age"}}
			index2 := base.Index{Columns: []string{"name"}, Unique: true}

			client := new(Client)
			client.On("Close").Return()
			client.On("EnsureIndex", "users", index1).Return(nil)
			client.On("EnsureIndex", "users", index2).Return(nil)
			model.client = client

			assert.NotPanics(t, func() {
				model.EnsureIndex(index1, index2)
			})
		})
	})

	t.Run("postgres", func(t *testing.T) {
		config := base.DBConfig{Driver: base.PG}
		model := makeModel(&pg{}, config)
		t.Run("singleIndex", func(t *testing.T) {
			index := base.Index{Columns: []string{"age"}}

			client := new(Client)
			client.On("Close").Return()
			client.On("CreateTable", "pgs", pgStructure).Return(nil)
			client.On("EnsureIndex", "pgs", index).Return(nil)
			model.client = client

			assert.NotPanics(t, func() {
				model.EnsureIndex(index)
			})
		})

		t.Run("multipleIndex", func(t *testing.T) {
			index1 := base.Index{Columns: []string{"age"}}
			index2 := base.Index{Columns: []string{"name"}, Unique: true}

			client := new(Client)
			client.On("Close").Return()
			client.On("CreateTable", "pgs", pgStructure).Return(nil)
			client.On("EnsureIndex", "pgs", index1).Return(nil)
			client.On("EnsureIndex", "pgs", index2).Return(nil)
			model.client = client

			assert.NotPanics(t, func() {
				model.EnsureIndex(index1, index2)
			})
		})

		t.Run("typePanic", func(t *testing.T) {
			model := makeModel(&pgInvalid{}, config)
			index := base.Index{Columns: []string{"age"}}

			client := new(Client)
			client.On("Close").Return()
			model.client = client

			assert.Panics(t, func() {
				model.EnsureIndex(index)
			})
		})
	})

	t.Run("sqlServer", func(t *testing.T) {
		config := base.DBConfig{Driver: base.MSSQL}
		model := makeModel(&mssql{}, config)

		t.Run("singleIndex", func(t *testing.T) {
			index := base.Index{Columns: []string{"age"}}

			client := new(Client)
			client.On("Close").Return()
			client.On("CreateTable", "dbo.mssqls", mssqlStructure).Return(nil)
			client.On("EnsureIndex", "dbo.mssqls", index).Return(nil)
			model.client = client

			assert.NotPanics(t, func() {
				model.EnsureIndex(index)
			})
		})

		t.Run("multipleIndex", func(t *testing.T) {
			index1 := base.Index{Columns: []string{"age"}}
			index2 := base.Index{Columns: []string{"name"}, Unique: true}

			client := new(Client)
			client.On("Close").Return()
			client.On("CreateTable", "dbo.mssqls", mssqlStructure).Return(nil)
			client.On("EnsureIndex", "dbo.mssqls", index1).Return(nil)
			client.On("EnsureIndex", "dbo.mssqls", index2).Return(nil)
			model.client = client

			assert.NotPanics(t, func() {
				model.EnsureIndex(index1, index2)
			})
		})

		t.Run("typePanic", func(t *testing.T) {
			model := makeModel(&mssqlInvalid{}, config)
			index := base.Index{Columns: []string{"age"}}

			client := new(Client)
			client.On("Close").Return()
			model.client = client

			assert.Panics(t, func() {
				model.EnsureIndex(index)
			})
		})
	})
}

func TestModel_Find(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		config := base.DBConfig{Driver: base.PG}
		model := makeModel(&Profile{}, config)
		u := base.NewRecordData(
			[]string{"id", "name", "age", "status", "rate", "score", "worth"},
			base.RecordMap{
				"id": int64(1), "name": "Test", "age": int64(1), "status": false,
				"rate": 8.9, "score": int64(56), "worth": "7845421000000000000",
			},
		)

		client := new(Client)
		client.On("Close").Return()
		client.On("FindByID", "profiles", 1).Return(*u, nil)
		model.client = client

		res, err := model.Find(1)

		assert.Nil(t, err)
		assert.NotNil(t, res)

		p := res.(*Profile)

		assert.Equal(t, 1, p.ID)
		assert.Equal(t, "Test", p.Name)
		assert.Equal(t, 1, p.Age)
		assert.Equal(t, false, p.Status)
	})

	t.Run("postgresSpecialTypes", func(t *testing.T) {
		config := base.DBConfig{Driver: base.PG}
		model := makeModel(&Special{}, config)
		u := base.NewRecordData(
			[]string{
				"map", "struct", "slice_int", "slice_int_8", "slice_int_16",
				"slice_int_32", "slice_int_64", "slice_uint", "slice_uint_8",
				"slice_uint_16", "slice_uint_32", "slice_uint_64", "slice_float3_2",
				"slice_float_64", "slice_string", "slice_json", "slice_struct",
				"slilce_bool",
			},
			base.RecordMap{
				"map":            `{"a":"b", "c": 1, "d": { "e" : "f" }}`,
				"struct":         `{"field1": 1, "field2": "val"}`,
				"slice_int":      `{1,2,3}`,
				"slice_int_8":    `{1,2,3}`,
				"slice_int_16":   `{1,2,3}`,
				"slice_int_32":   `{1,2,3}`,
				"slice_int_64":   `{1,2,3}`,
				"slice_uint":     `{1,2,3}`,
				"slice_uint_8":   `{1,2,3}`,
				"slice_uint_16":  `{1,2,3}`,
				"slice_uint_32":  `{1,2,3}`,
				"slice_uint_64":  `{1,2,3}`,
				"slice_float_32": `{1.2,3.4,5.6}`,
				"slice_float_64": `{1.2,3.4,5.6}`,
				"slice_string":   `{a,b,c}`,
				"slice_json":     `{"{\"a\":\"b\"}","{\"c\":\"d\"}"}`,
				"slice_struct":   `{"{\"field1\": 1, \"field2\": \"val\"}","{\"field1\": 2, \"field2\": \"val2\"}"}`,
				"slice_bool":     "{t,t,f}",
			},
		)
		client := new(Client)
		client.On("Close").Return()
		client.On("FindByID", "specials", 1).Return(*u, nil)
		model.client = client

		res, err := model.Find(1)

		assert.Nil(t, err)
		assert.NotNil(t, res)

		p := res.(*Special)

		assert.Equal(t, "b", p.Map["a"])
		assert.Equal(t, float64(1), p.Map["c"])
		assert.IsType(t, make(map[string]interface{}), p.Map["d"])
		assert.IsType(t, inner{}, p.Struct)
		assert.Equal(t, 1, p.Struct.Field1)
		assert.Equal(t, "val", p.Struct.Field2)
		assert.IsType(t, make([]int, 0), p.SliceInt)
		assert.Equal(t, 1, p.SliceInt[0])
		assert.Equal(t, 2, p.SliceInt[1])
		assert.Equal(t, 3, p.SliceInt[2])
		assert.IsType(t, make([]int8, 0), p.SliceInt8)
		assert.Equal(t, int8(1), p.SliceInt8[0])
		assert.Equal(t, int8(2), p.SliceInt8[1])
		assert.Equal(t, int8(3), p.SliceInt8[2])
		assert.IsType(t, make([]int16, 0), p.SliceInt16)
		assert.Equal(t, int16(1), p.SliceInt16[0])
		assert.Equal(t, int16(2), p.SliceInt16[1])
		assert.Equal(t, int16(3), p.SliceInt16[2])
		assert.IsType(t, make([]int32, 0), p.SliceInt32)
		assert.Equal(t, int32(1), p.SliceInt32[0])
		assert.Equal(t, int32(2), p.SliceInt32[1])
		assert.Equal(t, int32(3), p.SliceInt32[2])
		assert.IsType(t, make([]int64, 0), p.SliceInt64)
		assert.Equal(t, int64(1), p.SliceInt64[0])
		assert.Equal(t, int64(2), p.SliceInt64[1])
		assert.Equal(t, int64(3), p.SliceInt64[2])
		assert.Equal(t, uint(1), p.SliceUint[0])
		assert.Equal(t, uint(2), p.SliceUint[1])
		assert.Equal(t, uint(3), p.SliceUint[2])
		assert.IsType(t, make([]uint8, 0), p.SliceUint8)
		assert.Equal(t, uint8(1), p.SliceUint8[0])
		assert.Equal(t, uint8(2), p.SliceUint8[1])
		assert.Equal(t, uint8(3), p.SliceUint8[2])
		assert.IsType(t, make([]uint16, 0), p.SliceUint16)
		assert.Equal(t, uint16(1), p.SliceUint16[0])
		assert.Equal(t, uint16(2), p.SliceUint16[1])
		assert.Equal(t, uint16(3), p.SliceUint16[2])
		assert.IsType(t, make([]uint32, 0), p.SliceUint32)
		assert.Equal(t, uint32(1), p.SliceUint32[0])
		assert.Equal(t, uint32(2), p.SliceUint32[1])
		assert.Equal(t, uint32(3), p.SliceUint32[2])
		assert.IsType(t, make([]uint64, 0), p.SliceUint64)
		assert.Equal(t, uint64(1), p.SliceUint64[0])
		assert.Equal(t, uint64(2), p.SliceUint64[1])
		assert.Equal(t, uint64(3), p.SliceUint64[2])
		assert.IsType(t, make([]float32, 0), p.SliceFloat32)
		assert.Equal(t, float32(1.2), p.SliceFloat32[0])
		assert.Equal(t, float32(3.4), p.SliceFloat32[1])
		assert.Equal(t, float32(5.6), p.SliceFloat32[2])
		assert.IsType(t, make([]float64, 0), p.SliceFloat64)
		assert.Equal(t, float64(1.2), p.SliceFloat64[0])
		assert.Equal(t, float64(3.4), p.SliceFloat64[1])
		assert.Equal(t, float64(5.6), p.SliceFloat64[2])
		assert.IsType(t, make([]string, 0), p.SliceString)
		assert.Equal(t, "a", p.SliceString[0])
		assert.Equal(t, "b", p.SliceString[1])
		assert.Equal(t, "c", p.SliceString[2])
		assert.IsType(t, make([]base.JSONMap, 0), p.SliceJSON)
		assert.Equal(t, "b", p.SliceJSON[0]["a"])
		assert.Equal(t, "d", p.SliceJSON[1]["c"])
		assert.IsType(t, make([]inner, 0), p.SliceStruct)
		assert.Equal(t, 1, p.SliceStruct[0].Field1)
		assert.Equal(t, "val", p.SliceStruct[0].Field2)
		assert.Equal(t, 2, p.SliceStruct[1].Field1)
		assert.Equal(t, "val2", p.SliceStruct[1].Field2)
		assert.IsType(t, make([]bool, 0), p.SliceBool)
		assert.Equal(t, true, p.SliceBool[0])
		assert.Equal(t, true, p.SliceBool[1])
		assert.Equal(t, false, p.SliceBool[2])
	})

	t.Run("unsupportedTypeForSetField", func(t *testing.T) {
		config := base.DBConfig{Driver: base.PG}
		model := makeModel(&Invalid{}, config)
		u := base.NewRecordData(
			[]string{"wrong"},
			base.RecordMap{
				"wrong": "doesnt't matter what is the type",
			},
		)

		client := new(Client)
		client.On("Close").Return()
		client.On("FindByID", "invalids", 1).Return(*u, nil)
		model.client = client

		assert.Panics(t, func() {
			_, _ = model.Find(1)
		})
	})

	t.Run("unsupportedSliceForSetField", func(t *testing.T) {
		config := base.DBConfig{Driver: base.PG}
		model := makeModel(&Invalid2{}, config)
		u := base.NewRecordData(
			[]string{"wrong"},
			base.RecordMap{
				"wrong": "doesnt't matter what is the type",
			},
		)

		client := new(Client)
		client.On("Close").Return()
		client.On("FindByID", "invalid_2", 1).Return(*u, nil)
		model.client = client

		assert.Panics(t, func() {
			_, _ = model.Find(1)
		})
	})

	t.Run("notFound", func(t *testing.T) {
		config := base.DBConfig{Driver: base.PG}
		model := makeModel(&Profile{}, config)

		client := new(Client)
		client.On("Close").Return()
		client.On("FindByID", "profiles", 98).Return(
			*base.ZeroRecordData(), errors.New("not found"),
		)
		model.client = client

		user, err := model.Find(98)

		assert.NotNil(t, err)
		assert.Nil(t, user)
	})
}

func TestModel_Where(t *testing.T) {
	config := base.DBConfig{Driver: base.PG}
	model := makeModel(&Profile{}, config)

	client := new(Client)
	client.On("Close").Return()
	builder := new(QueryBuilder)
	client.On("Query", "profiles", conditions[0], conditions[1]).Return(builder)
	model.client = client

	b := model.Where(conditions...)

	assert.Equal(t, builder, b)
}

func TestModel_Create(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		config := base.DBConfig{Driver: base.Mongo}
		model := makeModel(&User{}, config)

		rData := base.NewRecordData(
			[]string{"name", "age", "available"},
			base.RecordMap{"name": "Test", "age": 18, "available": false},
		)

		objectID := bson.NewObjectId()
		client := new(Client)
		client.On("Close").Return()
		client.On("Insert", "users", rData).Return(nil).
			Run(func(args mock.Arguments) {
				rd := args.Get(1).(*base.RecordData)
				rd.Set("id", objectID)
				rd.Set("age", int64(18))
			})
		model.client = client

		user := User{Name: "Test", Age: 18, Status: false}

		err := model.Create(&user)

		assert.Nil(t, err)
		assert.Equal(t, objectID, user.ID)
		assert.Equal(t, "Test", user.Name)
		assert.Equal(t, 18, user.Age)
		assert.Equal(t, false, user.Status)
	})

	t.Run("failed", func(t *testing.T) {
		config := base.DBConfig{Driver: base.Mongo}
		model := makeModel(&User{}, config)

		rData := base.NewRecordData(
			[]string{"name", "age", "available"},
			base.RecordMap{"name": "Test", "age": 18, "available": false},
		)

		client := new(Client)
		client.On("Close").Return()
		client.On("Insert", "users", rData).Return(errTest)
		model.client = client

		user := User{Name: "Test", Age: 18, Status: false}

		err := model.Create(&user)

		assert.NotNil(t, err)
	})
}

func TestModel_Update(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		config := base.DBConfig{Driver: base.Mongo}
		model := makeModel(&User{}, config)

		rData := base.NewRecordData(
			[]string{"name", "age", "available"},
			base.RecordMap{"name": "Test", "age": 18, "available": false},
		)

		objectID := bson.NewObjectId()
		client := new(Client)
		client.On("Close").Return()
		client.On("UpdateByID", "users", objectID, *rData).Return(nil)
		model.client = client

		user := User{ID: objectID, Name: "Test", Age: 18, Status: false}

		err := model.Update(user)

		assert.Nil(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		config := base.DBConfig{Driver: base.Mongo}
		model := makeModel(&User{}, config)

		rData := base.NewRecordData(
			[]string{"name", "age", "available"},
			base.RecordMap{"name": "Test", "age": 18, "available": false},
		)

		objectID := bson.NewObjectId()
		client := new(Client)
		client.On("Close").Return()
		client.On("UpdateByID", "users", objectID, *rData).Return(errTest)
		model.client = client

		user := User{ID: objectID, Name: "Test", Age: 18, Status: false}

		err := model.Update(user)

		assert.NotNil(t, err)
	})
}

func TestModel_Delete(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		config := base.DBConfig{Driver: base.Mongo}
		model := makeModel(&User{}, config)

		objectID := bson.NewObjectId()
		client := new(Client)
		client.On("Close").Return()
		client.On("DeleteByID", "users", objectID).Return(nil)
		model.client = client

		user := User{ID: objectID, Name: "Test", Age: 18, Status: false}

		err := model.Delete(user)

		assert.Nil(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		config := base.DBConfig{Driver: base.Mongo}
		model := makeModel(&User{}, config)

		objectID := bson.NewObjectId()
		client := new(Client)
		client.On("Close").Return()
		client.On("DeleteByID", "users", objectID).Return(errTest)
		model.client = client

		user := User{ID: objectID, Name: "Test", Age: 18, Status: false}

		err := model.Delete(user)

		assert.NotNil(t, err)
	})
}

func TestModel_PrepareClient(t *testing.T) {
	t.Run("mongo", func(t *testing.T) {
		original := newMongo
		defer func() { newMongo = original }()

		config := base.DBConfig{Driver: base.Mongo}
		model := makeModel(&User{}, config)

		// mocking newMongo function
		newMongo = newMongoMock

		model.PrepareClient()

		assert.NotNil(t, model.client)
		assert.Implements(t, (*base.Client)(nil), model.client)
	})

	t.Run("postgres", func(t *testing.T) {
		original := newPostgres
		defer func() { newPostgres = original }()

		config := base.DBConfig{Driver: base.PG}
		model := makeModel(&User{}, config)

		// mocking newPostgres function
		newPostgres = newSQLClientMock

		model.PrepareClient()

		assert.NotNil(t, model.client)
		assert.Implements(t, (*base.Client)(nil), model.client)
	})

	t.Run("sqlServer", func(t *testing.T) {
		original := newSQLServer
		defer func() { newSQLServer = original }()

		config := base.DBConfig{Driver: base.MSSQL}
		model := makeModel(&User{}, config)

		// mocking newSQLServer function
		newSQLServer = newSQLClientMock

		model.PrepareClient()

		assert.NotNil(t, model.client)
		assert.Implements(t, (*base.Client)(nil), model.client)
	})

	t.Run("invalidDriver", func(t *testing.T) {
		config := base.DBConfig{Driver: "invalid"}
		model := makeModel(&User{}, config)

		assert.Panics(t, model.PrepareClient)
		assert.Nil(t, model.client)
	})
}
