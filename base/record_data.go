package base

// RecordMap is map of string-interface that represent data on a record
type RecordMap map[string]interface{}

// RecordData is a struct containing a map of string interface which
// represents data of a record in database and list of columns name
// used for keep map in order. It could be used for both upserting
// and fetching data from database. Map key represents the column
// name and its value represents the column value in database
type RecordData struct {
	data RecordMap
	keys []string
}

// ZeroRecordData instantiate the record data with zero values
func ZeroRecordData() *RecordData {
	data := RecordData{}
	data.Zero()
	return &data
}

// NewRecordData instantiate the record data with keys and data
func NewRecordData(keys []string, data RecordMap) *RecordData {
	return &RecordData{data: data, keys: keys}
}

// Length returns length of record data map
func (d *RecordData) Length() int {
	return len(d.data)
}

// GetColumns returns list of columns
func (d *RecordData) GetColumns() []string {
	return d.keys
}

// GetValues returns list of values enquoted by `enquoter`
func (d *RecordData) GetValues(enquoter Enquoter) []string {
	values := make([]string, 0, d.Length())
	for _, col := range d.keys {
		values = append(values, enquoter(d.data[col]))
	}

	return values
}

// Set sets `value` for `key` in record data map.
// It will replace the key value if it key is already exists.
func (d *RecordData) Set(key string, value interface{}) {
	// if data map and keys are nil we empty them
	if d.data == nil || d.keys == nil {
		d.Zero()
	}

	// if key is not exists on map, add it to keys
	if _, ok := d.data[key]; !ok {
		d.keys = append(d.keys, key)
	}

	d.data[key] = value
}

// Zero will empty all fields of record data
func (d *RecordData) Zero() {
	d.data = make(RecordMap)
	d.keys = make([]string, 0, 0)
}

// Get returns the value sets for `key`
func (d *RecordData) Get(key string) interface{} {
	return d.data[key]
}

// GetMap returns the data map
func (d *RecordData) GetMap() *RecordMap {
	return &d.data
}

// RecordDataSet is slice of RecordData represents results from db
type RecordDataSet []RecordData
