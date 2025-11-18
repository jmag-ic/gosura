package sql

import (
	"context"
	"fmt"
	"strings"

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

type ParseHookConfig struct {
	OperatorMap    map[string]string // Map of Hasura operators to SQL operators
	AggregateFnMap map[string]string // Map of aggregate function names to SQL functions
	NameDelimiter  string
	ConvertValueFn func(value gjson.Result) any
}

// SQLParseHook generates SQL WHERE clauses from Hasura filters
type SQLParseHook struct {
	Conditions   []string        // Main conditions array
	Params       []any           // Parameters for placeholders
	ParamIndex   int             // Current parameter index
	LogicalStack []*LogicalGroup // Stack of logical groups
	CurrentGroup *LogicalGroup   // Current logical group being processed
	OrderBy      []string        // Order by conditions
	Aggregates   []string        // Aggregate expressions
	Config       *ParseHookConfig
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

// NewSQLParseHook creates a new SQLWhereHook instance
func NewSQLParseHook(config *ParseHookConfig) *SQLParseHook {
	if config == nil {
		config = NewDefaultSQLParserHookConfig()
	}

	h := &SQLParseHook{
		Conditions:   make([]string, 0),
		Params:       make([]any, 0),
		ParamIndex:   1,
		LogicalStack: make([]*LogicalGroup, 0),
		OrderBy:      make([]string, 0),
		Aggregates:   make([]string, 0),
		Config:       config,
	}

	return h
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

// GetWhereClause returns the final WHERE clause and parameters
func (h *SQLParseHook) GetWhereClause() (string, []any) {
	// Join all conditions with AND
	whereClause := strings.Join(h.Conditions, fmt.Sprintf(" %s ", opAND))

	// Clean up any extra spaces
	whereClause = strings.TrimSpace(whereClause)

	return whereClause, h.Params
}

// GetOrderByClause returns the final ORDER BY clause
func (h *SQLParseHook) GetOrderByClause() string {
	return strings.Join(h.OrderBy, ", ")
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

	// Parse DISTINCT option
	hasDistinct := options.Exists() && options.Get("distinct").Bool()

	// Validate DISTINCT with wildcard
	if hasDistinct && field == "*" {
		return fmt.Errorf("DISTINCT can only be used with specific fields, not '*'")
	}

	// Build field expression
	distinct := ""
	if hasDistinct {
		distinct = "DISTINCT "
	}

	fieldAlias := "*"
	resultAlias := function

	if field != "*" {
		fieldAlias = h.getColumnAlias(field, []string{})
		resultAlias += "_" + field
	}

	expr := fmt.Sprintf("%s(%s%s) AS %s", sqlFn, distinct, fieldAlias, resultAlias)
	h.Aggregates = append(h.Aggregates, expr)

	return nil
}

// GetAggregates returns the aggregate expressions as a slice
func (h *SQLParseHook) GetAggregates() []string {
	return h.Aggregates
}

// getAggregateFunction returns the SQL aggregate function for a given name
func (h *SQLParseHook) getAggregateFunction(fn string) string {
	if h.Config != nil && h.Config.AggregateFnMap != nil {
		return h.Config.AggregateFnMap[fn]
	}
	return DefaultAggregateFnMap[fn]
}
