package gosura

import (
	"reflect"
	"strings"
)

// ColumnsMap is a type that defines a mapping between the field names and the column names of a table.
type ColumnsMap map[string]string

// JoinStruct is a struct that holds the information about a join.
type JoinStruct struct {
	Table string // Join table name
	FK    string // Join table foreign key
	Ref   string // Target table reference key
}

// JoinSpec is a struct that holds the information about a join from a table spec association.
// It contains the joins array that holds the information about the required joins to satisfy the association and the
// table spec of the associated table.
type JoinSpec struct {
	Joins     []JoinStruct
	TableSpec *TableSpec
}

// TableSpec is a struct that holds the information about a table in the database. It contains the table name,
// the columns dictionary and the associations with other tables.
type TableSpec struct {
	Name       string               // Table name
	JoinsMap   map[string]*JoinSpec // Map for the associations with other tables through the join spec. The key is the association field name
	ColumnsMap ColumnsMap           // It serves as a mapping between the field names and the column names of the table
}

// GetColumns returns the column names for the given fields using the columns map from the table spec.
func (t *TableSpec) GetColumns(fields []string) []string {
	columns := []string{}

	for _, field := range fields {
		if column, ok := t.ColumnsMap[field]; ok {
			columns = append(columns, column)
		}
	}

	return columns
}

// JoinSpecsOpts is a type that defines a build option for the table spec.
type JoinSpecsOpts func(*TableSpec)

// WithAssociation is a build option for the table spec that adds an association to the table spec.
// It takes the association field name and the join spec as arguments.
func WithAssociation(key string, joinSpec *JoinSpec) JoinSpecsOpts {
	return func(j *TableSpec) {
		j.JoinsMap[key] = joinSpec
	}
}

// NewTableSpec is a function that creates a new table spec with the given table name, columns map and options.
func NewTableSpec(tableName string, columnsMap ColumnsMap, opts ...JoinSpecsOpts) *TableSpec {
	tableSpec := &TableSpec{
		Name:       tableName,
		JoinsMap:   make(map[string]*JoinSpec),
		ColumnsMap: columnsMap,
	}
	// Apply options
	for _, opt := range opts {
		opt(tableSpec)
	}
	return tableSpec
}

// NewTableSpec usage example:
// var UsersTableSpec = NewTableSpec(
// 	"users",
// 	ColumnsMap{
// 		"id":        "id",
// 		"username":  "username",
// 		"updatedAt": "updated_at",
// 		"createdAt": "created_at",
// 	},
// 	WithAssociation("profile", &JoinSpec{
// 		Joins: []JoinStruct{
// 			{Table: "profiles", FK: "user_id", Ref: "id"},
// 		},
// 		TableSpec: ProfilesTableSpec,
// 	}),
// 	WithAssociation("roles", &JoinSpec{
// 		Joins: []JoinStruct{
// 			{Table: "users_roles", FK: "user_id", Ref: "id"},
// 			{Table: "roles", FK: "id", Ref: "role_id"},
// 		},
// 		TableSpec: RolesTableSpec,
// 	}))

// tablesRegister is a map that holds the table specs for the registered models.
var tablesRegister = make(map[string]*TableSpec)

// RegisterModels is a function that builds and stores the table specs for the given models using 'gosr' custom tags.
func RegisterModels(models ...interface{}) {
	for _, model := range models {
		registerTableSpecFromModel(model)
	}
}

// GetFromRegistry is a function that returns the table spec for the given model from the tables register.
func GetFromRegistry(model interface{}) *TableSpec {
	tableName := GetTableName(model)
	if tableSpec, ok := tablesRegister[tableName]; ok {
		return tableSpec
	}
	return nil
}

// Tabler is an interface that defines a TableName method that returns the table name of a model.
type Tabler interface {
	TableName() string
}

// GetTableName is a function that returns the table name of a model.
// If the model implements the Tabler interface, it returns the result of the TableName method.
// Otherwise, it returns the pluralized lowercase name of the model type.
func GetTableName(model interface{}) string {
	// Get table Name
	var tableName string
	if tabler, ok := model.(Tabler); !ok {

		// Iterate until we get the struct type
		modelType := reflect.TypeOf(model)
		for modelType.Kind() != reflect.Struct {
			modelType = modelType.Elem()
		}

		tableName = strings.ToLower(modelType.Name())
	} else {
		tableName = tabler.TableName()
	}
	return tableName
}

// registerTableSpecFromModel is a function that builds and stores the table spec for the given model using 'gosr' custom tags.
func registerTableSpecFromModel(model interface{}) *TableSpec {
	// Get table Name
	tableName := GetTableName(model)

	// Check if the table spec is already registered
	if tableSpec, ok := tablesRegister[tableName]; ok {
		// log.Trace().Msgf("Table spec for '%s; table is already registered", tableName)
		return tableSpec
	}

	// log.Trace().Msgf("Registering table spec for '%s' table", tableName)

	// Iterate until we get the struct type
	modelType := reflect.TypeOf(model)
	for modelType.Kind() != reflect.Struct {
		modelType = modelType.Elem()
	}

	columnsMap := ColumnsMap{}
	joinsMap := make(map[string]*JoinSpec)

	// Parse the fields of the struct
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		gosrTag := field.Tag.Get("gosr")
		jsonTag := field.Tag.Get("json")

		var fieldName string
		if jsonTag != "" {
			fieldName = strings.Split(jsonTag, ",")[0]
		} else {
			fieldName = strings.ToLower(field.Name)
		}

		// Add to ColumnsMap
		if gosrTag == "" {
			continue
		}

		if len(strings.Split(gosrTag, ";")) == 1 {
			columnsMap[fieldName] = gosrTag
			continue
		}

		// Iterate until we get the struct type
		fieldType := field.Type
		for fieldType.Kind() != reflect.Struct {
			fieldType = fieldType.Elem()
		}

		// Create a new instance of the field struct
		fieldStruct := reflect.New(fieldType).Elem().Interface()
		// Get the table spec of the nested struct
		nestedTableSpec := registerTableSpecFromModel(fieldStruct)
		// Get the join spec
		joinSpec := parseRelTag(gosrTag, nestedTableSpec)
		joinsMap[fieldName] = joinSpec
	}

	tableSpec := &TableSpec{
		Name:       tableName,
		JoinsMap:   joinsMap,
		ColumnsMap: columnsMap,
	}

	// Register the table spec
	tablesRegister[tableName] = tableSpec

	return tableSpec
}

// parseRelTag is a function that parses an association 'gosr' tag and returns the join spec.
func parseRelTag(tag string, tableSpec *TableSpec) *JoinSpec {
	parts := strings.Split(tag, ";")
	joinStructs := []JoinStruct{}

	for _, part := range parts {
		kv := strings.Split(part, ":")
		if len(kv) != 2 {
			continue
		}
		key := kv[0]
		value := kv[1]
		switch key {
		case "FK":
			if len(joinStructs) == 0 {
				joinStructs = append(joinStructs, JoinStruct{Table: tableSpec.Name})
			}
			joinStructs[0].FK = value
		case "Ref":
			if len(joinStructs) == 0 {
				joinStructs = append(joinStructs, JoinStruct{Table: tableSpec.Name})
			}
			joinStructs[0].Ref = value
		case "m2m":
			joinStructs = append(joinStructs, JoinStruct{Table: value})
		case "joinFK":
			if len(joinStructs) == 1 {
				joinStructs = append(joinStructs, JoinStruct{Table: tableSpec.Name})
			}
			// In m2m the joinFK is the ref of the target table
			joinStructs[1].Ref = value
		case "joinRef":
			if len(joinStructs) == 1 {
				joinStructs = append(joinStructs, JoinStruct{Table: tableSpec.Name})
			}
			// In m2m the joinRef is the fk of the intermediete table
			joinStructs[1].FK = value
		}
	}

	return &JoinSpec{
		Joins:     joinStructs,
		TableSpec: tableSpec,
	}
}
