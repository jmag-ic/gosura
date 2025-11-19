package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmag-ic/gosura/pkg/inspector"
	"github.com/tidwall/gjson"
)

// DefaultOperatorMap maps Hasura operators to SQL operators
var DefaultOperatorMap = map[string]string{
	"_eq":      "=",
	"_neq":     "!=",
	"_gt":      ">",
	"_lt":      "<",
	"_gte":     ">=",
	"_lte":     "<=",
	"_in":      "IN",
	"_nin":     "NOT IN",
	"_is_null": "IS NULL",
	"_like":    "LIKE",
	"_nlike":   "NOT LIKE",
	"_ilike":   "ILIKE",
	"_nilike":  "NOT ILIKE",
}

// DefaultAggregateFnMap maps aggregate function names to SQL aggregate functions
var DefaultAggregateFnMap = map[string]string{
	"count":    "COUNT",
	"sum":      "SUM",
	"avg":      "AVG",
	"max":      "MAX",
	"min":      "MIN",
	"stddev":   "STDDEV",
	"variance": "VARIANCE",
}

var DefaultNameDelimiter = `"`

var DefaultConvertValueFn = func(value gjson.Result) any {
	switch value.Type {
	case gjson.Number:
		// Try to convert to int first
		if float64(value.Int()) == value.Float() {
			return value.Int()
		}
		return value.Float()
	case gjson.String:
		return value.String()
	case gjson.True:
		return true
	case gjson.False:
		return false
	case gjson.Null:
		return nil
	case gjson.JSON:
		if !value.IsArray() {
			return value.Raw
		}
		return value.Value()

	default:
		return value.Value()
	}
}

// DefaultAggregateBuilder handles standard SQL aggregate functions.
// It supports DISTINCT and builds expressions like: COUNT(*) AS count
var DefaultAggregateBuilder = func(
	function string,
	sqlFunction string,
	field string,
	options gjson.Result,
	getColumnAlias func(string, []string) string,
) (string, string, error) {
	// Parse DISTINCT option
	hasDistinct := options.Exists() && options.Get("distinct").Bool()

	// Validate DISTINCT with wildcard
	if hasDistinct && field == "*" {
		return "", "", fmt.Errorf("DISTINCT can only be used with specific fields, not '*'")
	}

	// Build field expression
	distinct := ""
	if hasDistinct {
		distinct = "DISTINCT "
	}

	fieldAlias := "*"
	resultAlias := function

	if field != "*" {
		fieldAlias = getColumnAlias(field, []string{})
		resultAlias += "_" + strings.ReplaceAll(field, ".", "_")
	}

	return fmt.Sprintf("%s(%s%s)", sqlFunction, distinct, fieldAlias), resultAlias, nil
}

// AggregateExprBuilder is a function that builds aggregate SQL expressions.
// It receives the function name, SQL function name, field, options, and a helper
// to build column aliases. It returns the complete aggregate expression with alias.
type AggregateExprBuilder func(
	function string, // e.g., "string_agg"
	sqlFunction string, // e.g., "STRING_AGG" from map lookup
	field string, // e.g., "username"
	options gjson.Result, // Any additional options
	getColumnAlias func(string, []string) string, // Helper to build column names
) (expr string, alias string, err error)

type ParseHookConfig struct {
	OperatorMap        map[string]string    // Map of Hasura operators to SQL operators
	AggregateFnMap     map[string]string    // Map of aggregate function names to SQL functions
	AggregateBuilderFn AggregateExprBuilder // Optional custom aggregate expression builder
	NameDelimiter      string
	ConvertValueFn     func(value gjson.Result) any
}

// SQLParseHook generates SQL WHERE clauses from Hasura filters
type SQLParseHook struct {
	Conditions      []string          // Main conditions array
	Params          []any             // Parameters for placeholders
	ParamIndex      int               // Current parameter index
	LogicalStack    []*LogicalGroup   // Stack of logical groups
	CurrentGroup    *LogicalGroup     // Current logical group being processed
	OrderBy         []string          // Order by conditions
	Aggregates      map[string]string // Aggregate expressions
	AggregatesSlice []string          // Slice of aggregate expressions to preserve order
	Config          *ParseHookConfig
	Limit           *int // Optional limit for results
	Offset          *int // Optional offset for results
}

// Private types and constants for the hook implementation
type LogicalOperator string

const (
	opAND LogicalOperator = "AND"
	opOR  LogicalOperator = "OR"
	opNOT LogicalOperator = "NOT"
)

type LogicalGroup struct {
	operator   LogicalOperator // AND | OR | NOT
	operations []string        // The conditions in this group
}

func NewDefaultSQLParserHookConfig() *ParseHookConfig {
	return &ParseHookConfig{
		OperatorMap:    DefaultOperatorMap,
		AggregateFnMap: DefaultAggregateFnMap,
		NameDelimiter:  DefaultNameDelimiter,
		ConvertValueFn: DefaultConvertValueFn,
	}
}

// OnComparison handles field operations and converts them to SQL conditions
func (h *SQLParseHook) OnComparison(ctx context.Context, field string, operator string, value gjson.Result, path []string) error {
	// Get the column alias for the field
	alias := h.getColumnAlias(field, path)

	// Handle special cases first
	switch operator {
	case "_is_null":
		if value.Bool() {
			h.addCondition(fmt.Sprintf("%s IS NULL", alias))
		} else {
			h.addCondition(fmt.Sprintf("%s IS NOT NULL", alias))
		}
		return nil
	case "_in", "_nin":
		// Handle array values
		if !value.IsArray() {
			// TODO: handle error case
			return fmt.Errorf("array value expected, got %s", value.Type)
		}
		placeholders := make([]string, 0)
		values := value.Array()
		for _, v := range values {
			// Convert value to appropriate type
			paramValue := h.convertValue(v)
			h.Params = append(h.Params, paramValue)
			placeholders = append(placeholders, fmt.Sprintf("$%d", h.ParamIndex))
			h.ParamIndex++
		}
		sqlOp := h.getOperator(operator)
		h.addCondition(fmt.Sprintf("%s %s (%s)", alias, sqlOp, strings.Join(placeholders, ", ")))
		return nil
	}

	// Handle regular operators
	if sqlOp := h.getOperator(operator); sqlOp != "" {
		// Convert value to appropriate type
		paramValue := h.convertValue(value)
		h.Params = append(h.Params, paramValue)
		h.addCondition(fmt.Sprintf("%s %s $%d", alias, sqlOp, h.ParamIndex))
		h.ParamIndex++
		return nil
	}

	return fmt.Errorf("unsupported operator: %s", operator)
}

// OnNestedNodeStart tracks nested paths for proper column qualification
func (h *SQLParseHook) OnNestedNodeStart(ctx context.Context, field string, node gjson.Result, src string, path []string) {
}

// OnNestedNodeEnd handles nested node end
func (h *SQLParseHook) OnNestedNodeEnd(ctx context.Context, field string, node gjson.Result, src string, path []string) {
}

// OnLogicalGroupStart handles logical operators (_and, _or, _not)
func (h *SQLParseHook) OnLogicalGroupStart(ctx context.Context, operator string, node gjson.Result, path []string) error {
	switch operator {
	case "_and":
		h.pushLogicalGroup(opAND)
	case "_or":
		h.pushLogicalGroup(opOR)
	case "_not":
		h.pushLogicalGroup(opNOT)
	default:
		return fmt.Errorf("unsupported logical operator: %s", operator)
	}
	return nil
}

// OnLogicalGroupEnd is called when a logical group is complete
func (h *SQLParseHook) OnLogicalGroupEnd(ctx context.Context, operator string, node gjson.Result, path []string) {
	group := h.popLogicalGroup()
	if group == nil {
		return
	}

	// Default join operator is AND
	joinOp := opAND

	// If group operator isn't a NOT operator, use it as join operator
	// Otherwise, keep AND as default
	if group.operator != opNOT {
		joinOp = group.operator
	}

	condition := strings.Join(group.operations, fmt.Sprintf(" %s ", joinOp))
	if len(group.operations) > 1 {
		condition = fmt.Sprintf("(%s)", condition)
	}

	if group.operator == opNOT {
		condition = fmt.Sprintf("NOT %s", condition)
	}

	// Add the condition to the updated current group
	h.addCondition(condition)
}

func (h *SQLParseHook) OnOrderBy(ctx context.Context, field string, direction string, path []string) {
	// Get the column alias for the field
	alias := h.getColumnAlias(field, path)
	h.OrderBy = append(h.OrderBy, fmt.Sprintf("%s %s", alias, direction))
}

// addCondition adds a condition to the current group or main conditions
func (h *SQLParseHook) addCondition(condition string) {
	if h.CurrentGroup != nil {
		h.CurrentGroup.operations = append(h.CurrentGroup.operations, condition)
	} else {
		h.Conditions = append(h.Conditions, condition)
	}
}

// getColumnAlias returns the qualified column name for the given field
func (h *SQLParseHook) getColumnAlias(field string, path []string) string {
	d := DefaultNameDelimiter
	if h.Config != nil {
		d = h.Config.NameDelimiter
	}

	field = fmt.Sprintf("%s%s%s", d, strings.TrimSpace(field), d)
	fullPath := strings.Join(path, "__")
	if fullPath != "" {
		return fmt.Sprintf("%s%s%s.%s", d, fullPath, d, field)
	}

	return field
}

// popLogicalGroup pops the current logical group from the stack
func (h *SQLParseHook) popLogicalGroup() *LogicalGroup {
	if len(h.LogicalStack) == 0 || h.CurrentGroup == nil || len(h.CurrentGroup.operations) == 0 {
		return nil
	}
	// Pop the current group from the stack
	pop := h.LogicalStack[len(h.LogicalStack)-1]
	h.LogicalStack = h.LogicalStack[:len(h.LogicalStack)-1]

	// Update current group
	if len(h.LogicalStack) > 0 {
		h.CurrentGroup = h.LogicalStack[len(h.LogicalStack)-1]
	} else {
		h.CurrentGroup = nil
	}

	return pop
}

// pushLogicalGroup pushes a new logical group onto the stack
func (h *SQLParseHook) pushLogicalGroup(operator LogicalOperator) {
	group := &LogicalGroup{
		operator:   operator,
		operations: make([]string, 0),
	}
	h.LogicalStack = append(h.LogicalStack, group)
	h.CurrentGroup = group
}

// convertValue converts gjson.Result to appropriate Go type
func (h *SQLParseHook) convertValue(value gjson.Result) any {
	if h.Config != nil && h.Config.ConvertValueFn != nil {
		return h.Config.ConvertValueFn(value)
	}
	return DefaultConvertValueFn(value)
}

func (h *SQLParseHook) getOperator(op string) string {
	if h.Config != nil && h.Config.OperatorMap != nil {
		return h.Config.OperatorMap[op]
	}
	return DefaultOperatorMap[op]
}

// OnAggregateField processes an aggregate field and generates SQL
func (h *SQLParseHook) OnAggregateField(ctx context.Context, function string, field string, options gjson.Result) error {
	sqlFn := h.getAggregateFunction(function)
	if sqlFn == "" {
		return fmt.Errorf("unsupported aggregate function: %s", function)
	}

	// Use custom builder if provided, otherwise use default
	builder := h.getAggregateBuilder()
	expr, alias, err := builder(function, sqlFn, field, options, h.getColumnAlias)
	if err != nil {
		return err
	}

	// Store the aggregate expression
	h.Aggregates[alias] = expr
	h.AggregatesSlice = append(h.AggregatesSlice, alias)
	return nil
}

// getAggregateFunction returns the SQL aggregate function for a given name
func (h *SQLParseHook) getAggregateFunction(fn string) string {
	if h.Config != nil && h.Config.AggregateFnMap != nil {
		return h.Config.AggregateFnMap[fn]
	}
	return DefaultAggregateFnMap[fn]
}

// getAggregateBuilder returns the aggregate expression builder
func (h *SQLParseHook) getAggregateBuilder() AggregateExprBuilder {
	if h.Config != nil && h.Config.AggregateBuilderFn != nil {
		return h.Config.AggregateBuilderFn
	}
	return DefaultAggregateBuilder
}

// OnLimit implements the FilterHook interface for pagination
func (h *SQLParseHook) OnLimit(ctx context.Context, limit int) {
	h.Limit = &limit
}

// OnOffset implements the FilterHook interface for pagination
func (h *SQLParseHook) OnOffset(ctx context.Context, offset int) {
	h.Offset = &offset
}

func (h *SQLParseHook) GetQueryBuilder() SQLQueryBuilder {
	return SQLQueryBuilder{
		Aggregates:      h.Aggregates,
		AggregatesSlice: h.AggregatesSlice,
		Conditions:      h.Conditions,
		OrderBy:         h.OrderBy,
		Limit:           h.Limit,
		Offset:          h.Offset,
		Params:          h.Params,
	}
}

// GetLimit returns the limit value if set, nil otherwise
func (h *SQLParseHook) GetLimit() *int {
	return h.Limit
}

// GetOffset returns the offset value if set, nil otherwise
func (h *SQLParseHook) GetOffset() *int {
	return h.Offset
}

// SQLQueryBuilder builds SQL queries from filter components
type SQLQueryBuilder struct {
	Aggregates      map[string]string // Map of aggregate expressions
	AggregatesSlice []string          // Slice of aggregate expressions to preserve order
	Conditions      []string          // Main conditions array
	OrderBy         []string          // Order by conditions
	Limit           *int              // Optional limit for results
	Offset          *int              // Optional offset for results
	Params          []any             // Parameters for placeholders
}

// Build constructs a SQL SELECT query from the query builder components
func (h *SQLQueryBuilder) Build(entity string, columns ...string) string {
	var builder strings.Builder
	builder.WriteString("SELECT ")

	selectedColumns := []string{}

	if len(h.Aggregates) > 0 {
		for _, alias := range h.AggregatesSlice {
			expr := h.Aggregates[alias]
			selectedColumns = append(selectedColumns, expr+" AS "+alias)
		}
	}
	if len(columns) > 0 {
		selectedColumns = append(selectedColumns, columns...)
	}

	if len(selectedColumns) == 0 {
		selectedColumns = append(selectedColumns, "*")
	}

	// Join selected columns
	builder.WriteString(strings.Join(selectedColumns, ", "))

	// From clause
	builder.WriteString(" FROM " + entity)

	// Where clause
	if len(h.Conditions) > 0 {
		builder.WriteString(" WHERE ")
		builder.WriteString(strings.Join(h.Conditions, " AND "))
	}

	if len(h.Aggregates) > 0 && len(columns) > 0 {
		builder.WriteString(" GROUP BY ")
		builder.WriteString(strings.Join(columns, ", "))
	}

	if len(h.OrderBy) > 0 {
		builder.WriteString(" ORDER BY ")
		builder.WriteString(strings.Join(h.OrderBy, ", "))
	}

	if h.Limit != nil {
		builder.WriteString(fmt.Sprintf(" LIMIT %d", *h.Limit))
	}

	if h.Offset != nil {
		builder.WriteString(fmt.Sprintf(" OFFSET %d", *h.Offset))
	}

	return builder.String()
}

// SQLFilter is a filter hook that can generate SQL queries
type SQLFilter interface {
	inspector.FilterHook
	GetQueryBuilder() SQLQueryBuilder
}

// NewSQLFilter creates a new SQL filter hook with the given configuration
func NewSQLFilter(config *ParseHookConfig) SQLFilter {
	if config == nil {
		config = NewDefaultSQLParserHookConfig()
	}

	h := &SQLParseHook{
		Conditions:   make([]string, 0),
		Params:       make([]any, 0),
		ParamIndex:   1,
		LogicalStack: make([]*LogicalGroup, 0),
		OrderBy:      make([]string, 0),
		Aggregates:   make(map[string]string),
		Config:       config,
	}

	return h
}
