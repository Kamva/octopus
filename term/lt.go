package term

// LessThan is a condition struct using for comparing
// field value in database is less than the given value
type LessThan struct {
	Field string
	Value interface{}
}

// GetField returns the field name
func (c LessThan) GetField() string {
	return c.Field
}

// GetValue return the value to compare
func (c LessThan) GetValue() interface{} {
	return c.Value
}
