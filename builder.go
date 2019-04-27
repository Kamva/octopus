package octopus

import "github.com/Kamva/octopus/base"

// Builder is a wrapper around QueryBuilder that convert RecordData object to
// model's related scheme.
type Builder struct {
	builder base.QueryBuilder
	scheme  base.Scheme
	client  base.Client
}

// NewBuilder instantiate Builder with given QueryBuilder
func NewBuilder(builder base.QueryBuilder, scheme base.Scheme, client base.Client) *Builder {
	return &Builder{builder: builder, scheme: scheme, client: client}
}

// OrderBy set the order of returning result in following command
func (b *Builder) OrderBy(sorts ...base.Sort) base.Builder {
	b.builder = b.builder.OrderBy(sorts...)

	return b
}

// Limit set the limit that determines how many results should be
// returned in the following fetch command.
func (b *Builder) Limit(n int) base.Builder {
	b.builder = b.builder.Limit(n)

	return b
}

// Skip set the starting offset of the following fetch command
func (b *Builder) Skip(n int) base.Builder {
	b.builder = b.builder.Skip(n)

	return b
}

// Count execute a count command that will return the number records in
// specified destination table. If the query conditions was empty, it
// returns number of all records un destination table.
func (b *Builder) Count() (int, error) {
	defer b.client.Close()

	return b.builder.Count()
}

// First fetch data of the first record that match with query conditions.
func (b *Builder) First() (base.Scheme, error) {
	defer b.client.Close()

	data, err := b.builder.First()
	if err != nil {
		return nil, err
	}

	fillScheme(b.scheme, *data.GetMap())

	return b.scheme, nil
}

// All returns results that match with query conditions in RecordDataSet
// format. If the query conditions was empty it will return all records
// in specified destination table or error if anything went wrong.
func (b *Builder) All() ([]base.Scheme, error) {
	defer b.client.Close()

	dataSet, err := b.builder.All()
	if err != nil {
		return nil, err
	}

	var schemeSet []base.Scheme
	for _, data := range dataSet {
		fillScheme(b.scheme, *data.GetMap())
		schemeSet = append(schemeSet, b.scheme)
	}

	return schemeSet, nil
}

// Update updates records that math with query conditions with `data` and
// returns number of affected rows and error if anything went wring. If
// the query condition was empty it'll update all records in destination
// table.
func (b *Builder) Update(data base.Scheme) (int, error) {
	defer b.client.Close()

	recordData := generateRecordData(data, false)

	return b.builder.Update(*recordData)
}

// Delete removes every records in destination table that match with condition
// query and returns number of affected rows and error if anything went wrong.
// It will removes all records inside destination table if no condition query
// was set.
func (b *Builder) Delete() (int, error) {
	defer b.client.Close()

	return b.builder.Delete()
}
