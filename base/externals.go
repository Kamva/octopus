package base

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// SQLDatabase is an interface for sql.DB and used for testing and mocking
type SQLDatabase interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
	Conn(ctx context.Context) (*sql.Conn, error)
	Driver() driver.Driver
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Ping() error
	PingContext(ctx context.Context) error
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats
}

// SQLRows is an interface for sql.Rows and used for testing and mocking
type SQLRows interface {
	Close() error
	ColumnTypes() ([]*sql.ColumnType, error)
	Columns() ([]string, error)
	Err() error
	Next() bool
	NextResultSet() bool
	Scan(dest ...interface{}) error
}

// MongoSession is an interface for mgo.Session and used for testing and mocking
type MongoSession interface {
	BuildInfo() (info mgo.BuildInfo, err error)
	Clone() *mgo.Session
	Close()
	Copy() *mgo.Session
	DB(name string) *mgo.Database
	DatabaseNames() (names []string, err error)
	EnsureSafe(safe *mgo.Safe)
	FindRef(ref *mgo.DBRef) *mgo.Query
	Fsync(async bool) error
	FsyncLock() error
	FsyncUnlock() error
	LiveServers() (addrs []string)
	Login(cred *mgo.Credential) error
	LogoutAll()
	Mode() mgo.Mode
	New() *mgo.Session
	Ping() error
	Refresh()
	ResetIndexCache()
	Run(cmd interface{}, result interface{}) error
	Safe() (safe *mgo.Safe)
	SelectServers(tags ...bson.D)
	SetBatch(n int)
	SetBypassValidation(bypass bool)
	SetCursorTimeout(d time.Duration)
	SetMode(consistency mgo.Mode, refresh bool)
	SetPoolLimit(limit int)
	SetPoolTimeout(timeout time.Duration)
	SetPrefetch(p float64)
	SetSafe(safe *mgo.Safe)
	SetSocketTimeout(d time.Duration)
	SetSyncTimeout(d time.Duration)
}

// MongoCollection is an interface for mgo.Collection and used for testing and mocking
type MongoCollection interface {
	Bulk() *mgo.Bulk
	Count() (n int, err error)
	Create(info *mgo.CollectionInfo) error
	DropAllIndexes() error
	DropCollection() error
	DropIndex(key ...string) error
	DropIndexName(name string) error
	EnsureIndex(index mgo.Index) error
	EnsureIndexKey(key ...string) error
	Find(query interface{}) *mgo.Query
	FindId(id interface{}) *mgo.Query
	Indexes() (indexes []mgo.Index, err error)
	Insert(docs ...interface{}) error
	NewIter(session *mgo.Session, firstBatch []bson.Raw, cursorID int64, err error) *mgo.Iter
	Pipe(pipeline interface{}) *mgo.Pipe
	Remove(selector interface{}) error
	RemoveAll(selector interface{}) (info *mgo.ChangeInfo, err error)
	RemoveId(id interface{}) error
	Repair() *mgo.Iter
	Update(selector interface{}, update interface{}) error
	UpdateAll(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error)
	UpdateId(id interface{}, update interface{}) error
	Upsert(selector interface{}, update interface{}) (info *mgo.ChangeInfo, err error)
	UpsertId(id interface{}, update interface{}) (info *mgo.ChangeInfo, err error)
	Watch(pipeline interface{}, options mgo.ChangeStreamOptions) (*mgo.ChangeStream, error)
	With(s *mgo.Session) *mgo.Collection
}

// MongoQuery is an interface for mgo.Query and used for testing and mocking
type MongoQuery interface {
	All(result interface{}) error
	Apply(change mgo.Change, result interface{}) (info *mgo.ChangeInfo, err error)
	Batch(n int) *mgo.Query
	Collation(collation *mgo.Collation) *mgo.Query
	Comment(comment string) *mgo.Query
	Count() (n int, err error)
	Distinct(key string, result interface{}) error
	Explain(result interface{}) error
	For(result interface{}, f func() error) error
	Hint(indexKey ...string) *mgo.Query
	Iter() *mgo.Iter
	Limit(n int) *mgo.Query
	LogReplay() *mgo.Query
	MapReduce(job *mgo.MapReduce, result interface{}) (info *mgo.MapReduceInfo, err error)
	One(result interface{}) (err error)
	Prefetch(p float64) *mgo.Query
	Select(selector interface{}) *mgo.Query
	SetMaxScan(n int) *mgo.Query
	SetMaxTime(d time.Duration) *mgo.Query
	Skip(n int) *mgo.Query
	Snapshot() *mgo.Query
	Sort(fields ...string) *mgo.Query
	Tail(timeout time.Duration) *mgo.Iter
}
