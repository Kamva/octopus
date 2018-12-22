package term

// GreaterThanEqual is a condition struct using for comparing
// field value in database is greater than or equal the given value
type GreaterThanEqual struct {
	Field string
	Value interface{}
}

// GetField returns the field name
func (c GreaterThanEqual) GetField() string {
	return c.Field
}

// GetValue return the value to compare
func (c GreaterThanEqual) GetValue() interface{} {
	return c.Value
}
