package octopus

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/globalsign/mgo/bson"

	"github.com/Kamva/nautilus"
	"github.com/Kamva/nautilus/excp"
	"github.com/Kamva/nautilus/url"
	"github.com/Kamva/octopus/base"
	"github.com/Kamva/octopus/clients"
)

var newMongo = clients.NewMongoDB
var newSQLServer = clients.NewSQLServer
var newPostgres = clients.NewPostgres

// Configurator is a function for configuring Model attributes.
// Usually it is used for adding indices or configure table
// name, or even configuring drivers with custom drivers
type Configurator func(*Model)

// Model is an object that responsible for interacting
type Model struct {
	scheme    base.Scheme
	tableName string
	config    base.DBConfig
	client    base.Client
}

// Initiate initialize the model and prepare it for interacting with database
func (m *Model) Initiate(scheme base.Scheme, config base.DBConfig, configurators ...Configurator) {
	if reflect.ValueOf(scheme).Kind() != reflect.Ptr {
		panic("scheme should passed by reference")
	}

	// Set basic attributes
	m.config = config
	m.tableName = m.guessTableName(scheme)
	m.scheme = scheme

	// run configurators to set custom value for model attributes
	for _, configure := range configurators {
		configure(m)
	}

	// At last check for table name prefix
	if config.HasPrefix() {
		m.tableName = config.Prefix + "_" + m.tableName
	}
}

// EnsureIndex checks for table/collection existence in database, if not found tries
// to create it. Then it ensures that given indices are exists on table/collection.
func (m *Model) EnsureIndex(indices ...base.Index) {
	m.PrepareClient()
	defer m.CloseClient()

	if m.config.Driver != base.Mongo {
		err := m.client.CreateTable(m.tableName, m.getTableStruct())
		excp.PanicIfErr(err)
	}

	for _, index := range indices {
		err := m.client.EnsureIndex(m.tableName, index)
		excp.PanicIfErr(err)
	}
}

// Find search for a record/document in model table/collection match with given ID
func (m *Model) Find(id interface{}) (base.Scheme, error) {
	m.PrepareClient()
	defer m.CloseClient()

	result, err := m.client.FindByID(m.tableName, id)

	if result.Length() == 0 {
		return nil, err
	}

	m.fillScheme(m.scheme, *result.GetMap())

	return m.scheme, err
}

// Where returns a Query Builder based on given conditions on model table/collection
// that you can fetch, update or delete records/document match the query.
func (m *Model) Where(query ...base.Condition) base.QueryBuilder {
	m.PrepareClient()
	defer m.CloseClient()

	return m.client.Query(m.tableName, query...)
}

// Create inserts the given filled scheme into model table/collection and return
// inserted record/document or error if there was any fault in data insertion.
func (m *Model) Create(data base.Scheme) error {
	m.PrepareClient()
	defer m.CloseClient()

	recordData := m.generateRecordData(data, true)
	err := m.client.Insert(m.tableName, recordData)

	if err != nil {
		return err
	}

	m.fillScheme(data, *recordData.GetMap())

	return nil
}

// Update find a record/document that match with data ID and updates its field
// with data values. It'll return error if anything went wrong during update
func (m *Model) Update(data base.Scheme) error {
	m.PrepareClient()
	defer m.CloseClient()

	recordData := m.generateRecordData(data, false)

	return m.client.UpdateByID(m.tableName, data.GetID(), *recordData)
}

// Delete find a record/document that match with data ID and remove it from
// related table/collection. It will return error if anything went wrong
func (m *Model) Delete(data base.Scheme) error {
	m.PrepareClient()
	defer m.CloseClient()

	return m.client.DeleteByID(m.tableName, data.GetID())
}

// Guess the table name based on scheme name
func (m *Model) guessTableName(scheme base.Scheme) string {
	table := nautilus.Plural(nautilus.ToSnake(nautilus.GetType(scheme)))

	// If driver is SQL Server we should guess table name differently
	// since in SQL Server table names have an additional schema part
	if m.config.Driver == base.MSSQL {
		// If scheme is implement MsScheme we get schema name from scheme
		// itself, otherwise we chose a default schema which is `dbo`
		if msScheme, ok := scheme.(base.MsScheme); ok {
			table = msScheme.GetSchema() + "." + table
		} else {
			table = "dbo." + table
		}
	}

	return table
}

// PrepareClient Prepare client for further actions
func (m *Model) PrepareClient() {
	if m.client == nil {
		userInfo := url.NewUserInfo(m.config.Username, m.config.Password)

		switch m.config.Driver {
		case base.Mongo:
			i := &url.URL{
				Scheme:   "mongodb",
				UserInfo: userInfo,
				Host:     m.config.Host,
				Port:     m.config.Port,
				Path:     m.config.Database,
				Query:    m.config.GetOptions(),
			}
			con := i.String()
			m.client = newMongo(con, m.config.Database)
			break
		case base.MSSQL:
			m.config.AddOption("database", m.config.Database)
			i := &url.URL{
				Scheme:   "sqlserver",
				UserInfo: userInfo,
				Host:     m.config.Host,
				Port:     m.config.Port,
				Query:    m.config.GetOptions(),
			}
			con := i.String()
			m.client = newSQLServer(con)
			break
		case base.PG:
			i := &url.URL{
				Scheme:   "postgres",
				UserInfo: userInfo,
				Host:     m.config.Host,
				Port:     m.config.Port,
				Path:     m.config.Database,
				Query:    m.config.GetOptions(),
			}
			con := i.String()
			m.client = newPostgres(con)
			break
		default:
			panic("Invalid database driver")
		}
	}
}

// CloseClient close and destroy client connection
func (m *Model) CloseClient() {
	if m.client != nil {
		m.client.Close()
		m.client = nil
	}
}

func (m *Model) getTableStruct() base.TableStructure {
	fieldsData := m.getSchemeData(m.scheme)

	tableStructure := make([]base.FieldStructure, 0)
	for _, fieldData := range fieldsData {
		tagData := m.parseTag(fieldData.Tags.Get("sql"))

		if _, ok := tagData["ignore"]; !ok && !fieldData.Anonymous && fieldData.Exported {
			var fieldName string
			if name, ok := tagData["column"]; ok {
				fieldName = name
			} else {
				fieldName = nautilus.ToSnake(fieldData.Name)
			}

			if fieldName == m.scheme.GetKeyName() {
				tagData["ai"] = "true"
				tagData["id"] = "true"
				tagData["pk"] = "true"
			}

			fieldStructure := base.FieldStructure{
				Name:    fieldName,
				Type:    m.getMatchingType(fieldData.Type, tagData),
				Options: m.getFieldOptions(tagData),
			}

			tableStructure = append(tableStructure, fieldStructure)
		}
	}

	return tableStructure
}

func (m *Model) parseTag(tagValue string) base.SQLTag {
	valueSlice := strings.Split(tagValue, ";")
	tag := make(base.SQLTag)

	for _, slice := range valueSlice {
		if strings.Contains(slice, ":") {
			options := strings.Split(slice, ":")
			tag[options[0]] = options[1]
		} else {
			tag[slice] = "true"
		}
	}

	return tag
}

func (m *Model) getMatchingType(t reflect.Type, tags base.SQLTag) string {
	if typename, ok := tags["type"]; ok {
		return typename
	}

	switch m.config.Driver {
	case base.PG:
		return m.getPostgresMatchingType(t, tags)
	case base.MSSQL:
		return m.getMSSQLMatchingType(t)
	}

	panic("Invalid database driver")
}

func (m *Model) getPostgresMatchingType(t reflect.Type, tags base.SQLTag) string {
	switch t.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int8, reflect.Int16, reflect.Uint8:
		if tags["ai"] == "true" {
			return "SMALLSERIAL"
		}

		return "SMALLINT"
	case reflect.Int32, reflect.Int, reflect.Uint16:
		if tags["ai"] == "true" {
			return "SERIAL"
		}
		return "INT"
	case reflect.Int64, reflect.Uint32, reflect.Uint:
		if tags["ai"] == "true" {
			return "BIGSERIAL"
		}
		return "BIGINT"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "FLOAT8"
	case reflect.Uint64:
		return "DECIMAL"
	case reflect.Array, reflect.Slice:
		return m.getPostgresMatchingType(t.Elem(), tags) + "[]"
	case reflect.Map, reflect.Struct:
		return "JSON"
	case reflect.String:
		return "TEXT"
	}

	panic(fmt.Sprintf("Field Type [%s] is not supported. Change type or ignore it with tag", t.Kind().String()))
}

func (m *Model) getMSSQLMatchingType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Bool:
		return "BIT"
	case reflect.Uint8:
		return "TINYINT"
	case reflect.Int8, reflect.Int16:
		return "SMALLINT"
	case reflect.Int32, reflect.Int, reflect.Uint16:
		return "INT"
	case reflect.Int64, reflect.Uint32, reflect.Uint:
		return "BIGINT"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "FLOAT"
	case reflect.Uint64:
		return "DECIMAL"
	case reflect.String:
		return "NVARCHAR(MAX)"
	}

	panic(fmt.Sprintf("Field Type [%s] is not supported. Change type or ignore it with tag", t.Kind().String()))
}

func (m *Model) getFieldOptions(tags base.SQLTag) string {
	switch m.config.Driver {
	case base.PG:
		return m.getPostgresFieldOptions(tags)
	case base.MSSQL:
		return m.getMSSQLFieldOptions(tags)
	}

	panic("Invalid database driver")
}

func (m *Model) getPostgresFieldOptions(tags base.SQLTag) (options string) {
	if _, ok := tags["pk"]; ok {
		options = "PRIMARY KEY "
	} else if _, ok := tags["notnull"]; ok {
		options += "NOT NULL "
	} else if _, ok := tags["null"]; ok {
		options += "NULL "
	}

	if check, ok := tags["check"]; ok {
		options += fmt.Sprintf("CHECK (%s) ", check)
	}

	if def, ok := tags["default"]; ok {
		options += fmt.Sprintf("DEFAULT %s ", def)
	}

	if _, ok := tags["unique"]; ok {
		options += "UNIQUE"
	}

	return strings.TrimRight(options, " ")
}

func (m *Model) getMSSQLFieldOptions(tags base.SQLTag) (options string) {
	if val, ok := tags["id"]; ok {
		if val == "true" {
			options += "IDENTITY "
		} else {
			options += fmt.Sprintf("IDENTITY%s ", val)
		}
	}

	if _, ok := tags["pk"]; ok {
		options += "PRIMARY KEY "
		if _, ok := tags["cluster"]; ok {
			options += "CLUSTERED "
		} else if _, ok := tags["noncluster"]; ok {
			options += "NONCLUSTERED "
		}
	} else if _, ok := tags["null"]; ok {
		options += "NULL "
	} else if _, ok := tags["notnull"]; ok {
		options += "NOT NULL "
	}

	if def, ok := tags["default"]; ok {
		options += fmt.Sprintf("DEFAULT %s ", def)
	}

	if _, ok := tags["unique"]; ok {
		options += "UNIQUE "
		if _, ok := tags["cluster"]; ok {
			options += "CLUSTERED "
		} else if _, ok := tags["noncluster"]; ok {
			options += "NONCLUSTERED "
		}

	}

	if check, ok := tags["check"]; ok {
		options += fmt.Sprintf("CHECK %s ", check)
	}

	options = strings.TrimRight(options, " ")

	return options
}

func (m *Model) getSchemeData(scheme base.Scheme) []nautilus.FieldData {
	fieldsData, err := nautilus.GetStructFieldsData(scheme)
	excp.PanicIfErrCustomMsg(err, fmt.Sprintf("Invalid scheme %v", m.scheme))
	return fieldsData
}

func (m *Model) fillScheme(scheme base.Scheme, data base.RecordMap) {
	fieldsData := m.getSchemeData(scheme)

	for _, fieldData := range fieldsData {
		tagData := m.parseTag(fieldData.Tags.Get("sql"))

		if _, ok := tagData["ignore"]; !ok && !fieldData.Anonymous && fieldData.Exported {
			var fieldName string
			if name, ok := tagData["column"]; ok {
				fieldName = name
			} else {
				fieldName = nautilus.ToSnake(fieldData.Name)
			}

			m.setFieldValue(scheme, fieldData.Name, data[fieldName])
		}
	}
}

func (m *Model) setFieldValue(scheme base.Scheme, field string, value interface{}) {
	v := reflect.ValueOf(scheme).Elem()

	fieldVal := v.FieldByName(field)

	switch fieldVal.Kind() {
	case reflect.Bool:
		fieldVal.SetBool(value.(bool))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fieldVal.SetInt(value.(int64))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		fieldVal.SetUint(uint64(value.(int64)))
	case reflect.Uint64:
		f64, err := strconv.ParseFloat(value.(string), 64)
		excp.PanicIfErr(err)
		fieldVal.SetUint(uint64(f64))
	case reflect.Float32, reflect.Float64:
		fieldVal.SetFloat(value.(float64))
	case reflect.String:
		if oid, ok := value.(bson.ObjectId); ok {
			fieldVal.Set(reflect.ValueOf(oid))
		} else {
			fieldVal.SetString(value.(string))
		}
	case reflect.Map:
		data := fieldVal.Addr().Interface().(*base.JSONMap)
		err := json.Unmarshal([]byte(value.(string)), data)
		excp.PanicIfErr(err)
	case reflect.Array, reflect.Slice:
		valBytes := []byte(value.(string))
		s := string(valBytes[1 : len(valBytes)-1])

		// if value contains `","` it means that it is array of json
		// so for preventing conflict in splitting the string we
		// replace `","` with `"|"` and split string by |
		values := make([]string, 0)
		if strings.Contains(s, `","`) {
			s = strings.Replace(s, `","`, `"|"`, -1)
			values = strings.Split(s, "|")
		} else {
			values = strings.Split(s, ",")
		}

		slice := reflect.MakeSlice(fieldVal.Type(), len(values), len(values))

		for i, value := range values {
			x := slice.Index(i)
			x.Set(m.makeSliceValue(x, value))
		}

		fieldVal.Set(slice)
	case reflect.Struct:
		data := fieldVal.Addr().Interface()
		err := json.Unmarshal([]byte(value.(string)), data)
		excp.PanicIfErr(err)
	default:
		panic(fmt.Sprintf("Unsupported type [%s]", fieldVal.Type().String()))
	}
}

func (m *Model) makeSliceValue(elem reflect.Value, value string) reflect.Value {
	var cVal interface{}
	var err error

	switch elem.Kind() {
	case reflect.Bool:
		cVal, err = strconv.ParseBool(value)
	case reflect.Int:
		cVal, err = strconv.Atoi(value)
	case reflect.Int8:
		i64, e := strconv.ParseInt(value, 10, 8)
		cVal, err = int8(i64), e
	case reflect.Int16:
		i64, e := strconv.ParseInt(value, 10, 16)
		cVal, err = int16(i64), e
	case reflect.Int32:
		i64, e := strconv.ParseInt(value, 10, 32)
		cVal, err = int32(i64), e
	case reflect.Int64:
		cVal, err = strconv.ParseInt(value, 10, 64)
	case reflect.Uint:
		u64, e := strconv.ParseUint(value, 10, 64)
		cVal, err = uint(u64), e
	case reflect.Uint8:
		u64, e := strconv.ParseUint(value, 10, 8)
		cVal, err = uint8(u64), e
	case reflect.Uint16:
		u64, e := strconv.ParseUint(value, 10, 16)
		cVal, err = uint16(u64), e
	case reflect.Uint32:
		u64, e := strconv.ParseUint(value, 10, 32)
		cVal, err = uint32(u64), e
	case reflect.Uint64:
		cVal, err = strconv.ParseUint(value, 10, 64)
	case reflect.Float32:
		f64, e := strconv.ParseFloat(value, 64)
		cVal, err = float32(f64), e
	case reflect.Float64:
		cVal, err = strconv.ParseFloat(value, 64)
	case reflect.String:
		cVal, err = value, nil
	case reflect.Map:
		jsonMap := make(base.JSONMap)
		value = strings.Replace(value, "\\", "", -1)
		err = json.Unmarshal([]byte(value)[1:len(value)-1], &jsonMap)
		cVal = jsonMap
	case reflect.Struct:
		data := elem.Addr().Interface()
		value = strings.Replace(value, "\\", "", -1)
		err = json.Unmarshal([]byte(value)[1:len(value)-1], data)
		excp.PanicIfErr(err)
		return reflect.ValueOf(data).Elem()
	default:
		panic(fmt.Sprintf("Unsupported type [[]%s]", elem.Type().String()))
	}

	excp.PanicIfErr(err)
	return reflect.ValueOf(cVal)
}

func (m *Model) generateRecordData(scheme base.Scheme, insert bool) *base.RecordData {
	fieldsData := m.getSchemeData(scheme)
	data := base.ZeroRecordData()

	for _, fieldData := range fieldsData {
		tagData := m.parseTag(fieldData.Tags.Get("sql"))

		if _, ok := tagData["ignore"]; !ok && !fieldData.Anonymous && fieldData.Exported {
			var fieldName string
			if name, ok := tagData["column"]; ok {
				fieldName = name
			} else {
				fieldName = nautilus.ToSnake(fieldData.Name)
			}

			if (insert && m.isEmpty(fieldData.Value)) || (!insert && fieldName == scheme.GetKeyName()) {
				continue
			}

			data.Set(fieldName, fieldData.Value)
		}
	}

	return data
}

func (m *Model) isEmpty(value interface{}) bool {
	if oid, ok := value.(bson.ObjectId); ok {
		value = oid.Hex()
	}

	return value == nil || value == ""
}
