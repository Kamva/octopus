package octopus

// Scheme is the base scheme that other schemes can embed to have default methods
type Scheme struct{}

// GetKeyName return the name of primary key field name
func (s Scheme) GetKeyName() string {
	return "id"
}

// MongoScheme is the base scheme, same as Scheme, for mongo db schemes
type MongoScheme struct{}

// GetKeyName return the name of primary key field name
func (s MongoScheme) GetKeyName() string {
	return "_id"
}
