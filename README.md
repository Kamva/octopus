# Octopus

Octopus is an ORM/ODM written in Golang. It supports SQL and NoSQL databases and is easy to use.

## Get Started

Octopus is dependent to another library called nautilus. For getting nautilus run the following command:

```
go get -u github.com/kamva/nautilus
```

Then run the following command to get octopus package

```
go get -u github.com/kamva/octopus
```

## Usage

For using octopus you need a scheme and a model. Scheme represent the field in your desired table (or collection),
and Model is the struct that interact with the database.

Note that the scheme must implement `octopus/base.Scheme` interface. The model struct must embed the `octopus.Model`
and run `Initiate` method on its constructor.

```go
package models

import (
    "github.com/kamva/octopus"
    "github.com/kamva/octopus/base"
)


type User struct {
    octopus.Scheme // This is optional. This only adds GetKeyName method implementation that returns `id` by default
    ID          int     `sql:"pk"`
    Name        string  `sql:"column:full_name"`
    Email       string  `sql:"unique"`
    Password    string
    RawData     map[string]string `sql:"ignore"` // Add ignore tag if the field does not exists on table
}

func (u User) GetID() interface{} {
	return u.ID
}

type UserModel struct {
    octopus.Model
}

func NewUserModel() *UserModel {
    model :=  &UserModel{}
    config := base.DBConfig{Driver:base.PG, Host:"localhost", Port: "5432", Database: "MyDatabase"}
    model.Initiate(User{}, config)
    
    return model
}
```

Then you can use model like this:

```go
package main

import (
    "github.com/kamva/octopus/term"
	"models"
)


func main() {
	model := models.NewUserModel()
	
	// Find a user by ID
	user, err := model.Find(1)
	if err != nil {
		panic(err)
	}
	
	// Create a new record
	newUser := User{Name: "John Doe", Email: "john.doe@email.com", Password: "HashedPassword"}
	model.Create(&newUser)
	
	// Update a record
	user.Name = "New Name"
	model.Update(user)
	
	// Delete a record
	model.Delete(user)
	
	// Query the table
	model.Where(term.Equal{Field: "name", Value: "John Doe"}).First()
}
``` 

## Supported Databases

- [x] MongoDB
    - [x] Data Modelling
    - [ ] Raw Query
    - [ ] Aggregations
    - [ ] Relation Support [via lookup aggregation]
- [x] PostgreSQL
    - [x] Data Modelling
    - [x] Arrays and Json type support
    - [ ] Grouping
    - [ ] Raw Query
    - [ ] Relation Support
- [x] MSSQL
    - [x] Data Modelling
    - [ ] Grouping
    - [ ] Raw Query
    - [ ] Relation Support
    - [ ] Stored Procedures
- [ ] MySQL
- [ ] SQLite3
