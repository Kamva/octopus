package term

// GreaterThan is a condition struct using for comparing
// field value in database is greater than the given value
type GreaterThan struct {
	Field string
	Value interface{}
}

// GetField returns the field name
func (c GreaterThan) GetField() string {
	return c.Field
}

// GetValue return the value to compare
func (c GreaterThan) GetValue() interface{} {
	return c.Value
}
