package term

// IsNull is a condition struct using for checking
// field value in database is a null value
type IsNull struct {
	Field string
}

// GetField returns the field name
func (c IsNull) GetField() string {
	return c.Field
}

// GetValue return the value to compare
func (c IsNull) GetValue() interface{} {
	return nil
}
