package term

// NotEqual is a condition struct using for comparing
// field value in database is not equal to given value
type NotEqual struct {
	Field string
	Value interface{}
}

// GetField returns the field name
func (c NotEqual) GetField() string {
	return c.Field
}

// GetValue return the value to compare
func (c NotEqual) GetValue() interface{} {
	return c.Value
}
