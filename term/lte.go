package term

// LessThanEqual is a condition struct using for comparing
// field value in database is less than or equal the given value
type LessThanEqual struct {
	Field string
	Value interface{}
}

// GetField returns the field name
func (c LessThanEqual) GetField() string {
	return c.Field
}

// GetValue return the value to compare
func (c LessThanEqual) GetValue() interface{} {
	return c.Value
}
