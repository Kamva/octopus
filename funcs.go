package octopus

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/Kamva/nautilus"
	"github.com/Kamva/nautilus/types"
	"github.com/Kamva/octopus/base"
	"github.com/Kamva/shark"
	"github.com/globalsign/mgo/bson"
)

func fillScheme(scheme base.Scheme, data base.RecordMap) {
	fieldsData := getSchemeData(scheme)

	for _, fieldData := range fieldsData {
		tagData := parseTag(fieldData)

		if _, ok := tagData["ignore"]; !ok && !fieldData.Anonymous && fieldData.Exported {
			var fieldName string
			if name, ok := tagData["column"]; ok {
				fieldName = name
			} else {
				fieldName = nautilus.ToSnake(fieldData.Name)
			}

			if _, ok := data[fieldName]; ok {
				setFieldValue(scheme, fieldData.Name, data[fieldName])
			}
		}
	}
}

func getSchemeData(scheme base.Scheme) []nautilus.FieldData {
	fieldsData, err := nautilus.GetStructFieldsData(scheme)
	shark.PanicIfErrorWithMessage(err, fmt.Sprintf("Invalid scheme %v", scheme))
	return fieldsData
}

func parseTag(data nautilus.FieldData) base.SQLTag {
	tagValue := data.Tags.Get("sql")
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

	// check for bson tag, if present it can be used as column tag
	tagValue = data.Tags.Get("bson")
	if tagValue != "" {
		tag["column"] = tagValue
	}

	return tag
}

func setFieldValue(scheme base.Scheme, field string, value interface{}) {
	v := reflect.ValueOf(scheme).Elem()

	fieldVal := v.FieldByName(field)

	switch fieldVal.Kind() {
	case reflect.Bool:
		fieldVal.SetBool(value.(bool))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if val, ok := value.(int); ok {
			fieldVal.SetInt(int64(val))
		} else if val, ok := value.(int8); ok {
			fieldVal.SetInt(int64(val))
		} else if val, ok := value.(int16); ok {
			fieldVal.SetInt(int64(val))
		} else if val, ok := value.(int32); ok {
			fieldVal.SetInt(int64(val))
		} else {
			fieldVal.SetInt(value.(int64))
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		if val, ok := value.(uint); ok {
			fieldVal.SetUint(uint64(val))
		} else if val, ok := value.(uint8); ok {
			fieldVal.SetUint(uint64(val))
		} else if val, ok := value.(uint16); ok {
			fieldVal.SetUint(uint64(val))
		} else if val, ok := value.(uint32); ok {
			fieldVal.SetUint(uint64(val))
		} else {
			// Most Databases treated uint types as int type
			if val, ok := value.(int); ok {
				fieldVal.SetUint(uint64(val))
			} else if val, ok := value.(int8); ok {
				fieldVal.SetUint(uint64(val))
			} else if val, ok := value.(int16); ok {
				fieldVal.SetUint(uint64(val))
			} else if val, ok := value.(int32); ok {
				fieldVal.SetUint(uint64(val))
			} else {
				fieldVal.SetUint(uint64(value.(int64)))
			}
		}
	case reflect.Uint64:
		f64, err := strconv.ParseFloat(value.(string), 64)
		shark.PanicIfError(err)
		fieldVal.SetUint(uint64(f64))
	case reflect.Float32, reflect.Float64:
		if val, ok := value.(float32); ok {
			fieldVal.SetFloat(float64(val))
		} else {
			fieldVal.SetFloat(value.(float64))
		}
	case reflect.String:
		if oid, ok := value.(bson.ObjectId); ok {
			fieldVal.Set(reflect.ValueOf(oid))
		} else {
			fieldVal.SetString(value.(string))
		}
	case reflect.Map:
		// If the value is string, it is probably, a serialized format of map.
		if strVal, ok := value.(string); ok {
			data := fieldVal.Addr().Interface().(*types.JSONMap)
			err := json.Unmarshal([]byte(strVal), data)
			shark.PanicIfError(err)
		} else {
			b, _ := json.Marshal(value)
			field := fieldVal.Addr().Interface()
			_ = json.Unmarshal(b, field)
		}
	case reflect.Array, reflect.Slice:
		// If field type is slice or array and returning data is a string, it is
		// possible that an array data is saved serialized array. (Mostly arrays
		// in PostgreSQL)
		if strVal, ok := value.(string); ok {
			// Remove start and end character from
			valBytes := []byte(strVal)
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
				x.Set(makeSliceValue(x, value))
			}

			fieldVal.Set(slice)
		} else {
			// Here, we assume that the returning data is slice or array.
			fieldVal.Set(reflect.ValueOf(value))
		}
	case reflect.Struct:
		data := fieldVal.Addr().Interface()
		var b []byte
		var err error

		// Check if the value is the serialization of field value, convert the
		// string to bytes (PostgreSQL). If not (struct or map value for MongoDB
		// sub documents) we serialize the struct or map to json and then in both
		// situations, deserialize the bytes to defined struct format in field.
		if strVal, ok := value.(string); ok {
			b = []byte(strVal)
		} else {
			b, err = json.Marshal(value)
		}

		err = json.Unmarshal(b, data)
		shark.PanicIfError(err)
	case reflect.Ptr:
		if value != nil {
			rv := reflect.New(reflect.TypeOf(value))
			rv.Elem().Set(reflect.ValueOf(value))
			fieldVal.Set(rv)
		}
	default:
		panic(fmt.Sprintf("Unsupported type [%s]", fieldVal.Type().String()))
	}
}

func generateRecordData(scheme base.Scheme, insert bool) *base.RecordData {
	fieldsData := getSchemeData(scheme)
	data := base.ZeroRecordData()

	for _, fieldData := range fieldsData {
		tagData := parseTag(fieldData)

		if _, ok := tagData["ignore"]; !ok && !fieldData.Anonymous && fieldData.Exported {
			var fieldName string
			if name, ok := tagData["column"]; ok {
				fieldName = name
			} else {
				fieldName = nautilus.ToSnake(fieldData.Name)
			}

			// If we are inserting, new record we should skip empty columns if it
			// is set as null, or if it is empty ObjectID when driver is set to
			// mongodb.
			// If we are updating, we should only skip identifier field, despite
			// of its value.
			_, nullable := tagData["null"]
			if shouldSkipField(insert, nullable, fieldData.Value, fieldName, scheme) {
				continue
			}

			data.Set(fieldName, fieldData.Value)
		}
	}

	return data
}

func shouldSkipField(insert bool, nullable bool, value interface{}, fieldName string, scheme base.Scheme) bool {
	if fieldName == scheme.GetKeyName() {
		return true
	} else if insert {
		return (nullable && isZero(value)) || (isObjectID(value) && isZero(value))
	}

	return false
}

func makeSliceValue(elem reflect.Value, value string) reflect.Value {
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
		jsonMap := make(types.JSONMap)
		value = strings.Replace(value, "\\", "", -1)
		err = json.Unmarshal([]byte(value)[1:len(value)-1], &jsonMap)
		cVal = jsonMap
	case reflect.Struct:
		data := elem.Addr().Interface()
		value = strings.Replace(value, "\\", "", -1)
		err = json.Unmarshal([]byte(value)[1:len(value)-1], data)
		shark.PanicIfError(err)
		return reflect.ValueOf(data).Elem()
	default:
		panic(fmt.Sprintf("Unsupported type [[]%s]", elem.Type().String()))
	}

	shark.PanicIfError(err)
	return reflect.ValueOf(cVal)
}

func isZero(value interface{}) bool {
	t := reflect.TypeOf(value)
	if !t.Comparable() {
		return false
	}
	return value == reflect.Zero(t).Interface()
}

func isObjectID(value interface{}) bool {
	_, ok := value.(bson.ObjectId)

	return ok
}
