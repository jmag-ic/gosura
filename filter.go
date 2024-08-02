package gosura

import (
	"context"
	"fmt"
	"strings"
)

type filterCtx struct{}

// WithFilter creates a new context with the provided filter value and returns it
func WithFilter(ctx context.Context, filter *string) context.Context {
	return context.WithValue(ctx, filterCtx{}, filter)
}

// FilterFor returns the filter from the context
func FilterFor(ctx context.Context) *string {
	return ctx.Value(filterCtx{}).(*string)
}

// FilterInput represents the input filter structure
type FilterInput struct {
	DistinctOn []string               `json:"distinctOn"`
	Where      map[string]interface{} `json:"where"`
	OrderBy    interface{}            `json:"orderBy"`
	Offset     *int                   `json:"offset"`
	Limit      *int                   `json:"limit"`
}

// Filter is the parsed filter structure
type Filter struct {
	Args       []interface{}          // Arguments for the query added through the And and Or methods
	Columns    []string               // Columns to be selected
	DistinctOn []string               // Distinct on columns
	Extracts   map[string]interface{} // Contains the fields that were ignored by the parser
	Joins      []string               // Joins to be added to the query
	JoinsMap   map[string][]int       // Map of join alias to the indexes in the joins slice
	Limit      *int                   // Limit clause
	Offset     *int                   // Offset cluase
	OrderBy    string                 // Order by clause
	Table      string                 // Table to be queried
	Where      string                 // Where clause
	complexity int                    // Complexity in deep level of the query
}

// AddColumns adds columns for the select statement
func (f *Filter) AddColumns(columns ...string) *Filter {
	f.Columns = append(f.Columns, columns...)
	return f
}

// And adds an AND condition to the filter
func (f *Filter) And(condition string, args ...interface{}) *Filter {
	f.Args = append(f.Args, args...)

	if f.Where == "" {
		f.Where = condition
		return f
	}

	f.Where = fmt.Sprintf("%s AND (%s)", f.Where, condition)
	return f
}

// GetQuery returns the query string
func (f *Filter) GetQuery() string {
	query := ""

	if f.Table != "" {
		distinctOn := ""
		if len(f.DistinctOn) > 0 {
			distinctOn = fmt.Sprintf(" DISTINCT ON (%s)", strings.Join(f.DistinctOn, ","))
		}

		columns := "*"
		if len(f.Columns) > 0 {
			columns = strings.Join(f.Columns, ", ")
		}
		query = fmt.Sprintf("SELECT%s %s FROM %s", distinctOn, columns, f.Table)
	}

	if len(f.Joins) > 0 {
		query = fmt.Sprintf("%s %s", query, strings.Join(f.Joins, " "))
	}

	if f.Where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, f.Where)
	}

	if f.OrderBy != "" {
		query = fmt.Sprintf("%s ORDER BY %s", query, f.OrderBy)
	}

	if f.Limit != nil {
		query = fmt.Sprintf("%s LIMIT %d", query, *f.Limit)
	}

	if f.Offset != nil {
		query = fmt.Sprintf("%s OFFSET %d", query, *f.Offset)
	}

	return query
}

// Join adds a join sentence to the filter
func (f *Filter) Join(join string) *Filter {
	f.Joins = append(f.Joins, "JOIN "+join)
	return f
}

// Or adds an OR condition to the filter
func (f *Filter) Or(condition string) *Filter {
	if f.Where == "" {
		f.Where = condition
		return f
	}

	f.Where = fmt.Sprintf("%s OR (%s)", f.Where, condition)
	return f
}

// addJoinSpec adds a join to the filter using the join spec and returns the alias composed as {fieldName}__{tableName} of the joined table.
// this method is used during the string filter parsing
func (f *Filter) addJoinSpec(table, field string, joinSpec *JoinSpec) string {
	var idxs []int
	var joinAlias string
	currIdx := len(f.Joins)
	for i, join := range joinSpec.Joins {
		// Get the alias of the table to be joined
		joinAlias = fmt.Sprintf("%s__%s", field, join.Table)

		if _, ok := f.JoinsMap[joinAlias]; ok {
			continue
		}

		f.Joins = append(f.Joins, fmt.Sprintf("JOIN %s AS %s ON %s.%s = %s.%s", join.Table, joinAlias, table, join.Ref, joinAlias, join.FK))
		idxs = append(idxs, currIdx+i)
		f.JoinsMap[joinAlias] = idxs
		table = joinAlias
	}

	return joinAlias
}

// joinToLeftJoin converts the joins that resolve to the given alias to LEFT JOIN
func (f *Filter) joinToLeftJoin(alias string) {
	idxs := f.JoinsMap[alias]
	for _, idx := range idxs {
		f.Joins[idx] = strings.Replace(f.Joins[idx], "JOIN", "LEFT JOIN", 1)
	}
}
