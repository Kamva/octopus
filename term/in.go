package term

// In is a condition struct using for checking field
// value in database is in one of given values
type In struct {
	Field  string
	Values []interface{}
}

// GetField returns the field name
func (c In) GetField() string {
	return c.Field
}

// GetValue return the value to compare
func (c In) GetValue() interface{} {
	return c.Values
}
