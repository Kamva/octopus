package term

// NotNull is a condition struct using for checking
// field value in database is a null value
type NotNull struct {
	Field string
}

// GetField returns the field name
func (c NotNull) GetField() string {
	return c.Field
}

// GetValue return the value to compare
func (c NotNull) GetValue() interface{} {
	return nil
}
