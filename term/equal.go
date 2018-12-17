package term

// Equal is a condition struct using for comparing
// field value in database is equal to given value
type Equal struct {
	Field string
	Value interface{}
}

// GetField returns the field name
func (c Equal) GetField() string {
	return c.Field
}

// GetValue return the value to compare
func (c Equal) GetValue() interface{} {
	return c.Value
}
