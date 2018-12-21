package octopus

// Scheme is the base scheme that other schemes can embed to have default methods
type Scheme struct{}

// GetKeyName return the name of primary key field name
func (s Scheme) GetKeyName() string {
	return "id"
}
