package hooks

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

type SQLParserHookConfig struct {
	operatorMap    map[string]string // Map of Hasura operators to SQL operators
	nameDelimiter  string
	convertValueFn func(value gjson.Result) any
}

// SQLParseHook generates SQL WHERE clauses from Hasura filters
type SQLParseHook struct {
	conditions   []string        // Main conditions array
	params       []any           // Parameters for placeholders
	paramIndex   int             // Current parameter index
	logicalStack []*logicalGroup // Stack of logical groups
	currentGroup *logicalGroup   // Current logical group being processed
	orderBy      []string        // Order by conditions
	config       *SQLParserHookConfig
}

// Private types and constants for the hook implementation
type logicalOperator string

const (
	opAND logicalOperator = "AND"
	opOR  logicalOperator = "OR"
	opNOT logicalOperator = "NOT"
)

type logicalGroup struct {
	operator   logicalOperator // AND | OR | NOT
	operations []string        // The conditions in this group
}

func NewDefaultSQLParserHookConfig() *SQLParserHookConfig {
	return &SQLParserHookConfig{
		operatorMap:   DefaultOperatorMap,
		nameDelimiter: DefaultNameDelimiter,
	}
}

// NewSQLParseHook creates a new SQLWhereHook instance
func NewSQLParseHook(config *SQLParserHookConfig) *SQLParseHook {
	if config == nil {
		config = NewDefaultSQLParserHookConfig()
	}

	h := &SQLParseHook{
		conditions:   make([]string, 0),
		params:       make([]any, 0),
		paramIndex:   1,
		logicalStack: make([]*logicalGroup, 0),
		orderBy:      make([]string, 0),
		config:       config,
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
			h.params = append(h.params, paramValue)
			placeholders = append(placeholders, fmt.Sprintf("$%d", h.paramIndex))
			h.paramIndex++
		}
		sqlOp := h.getOperator(operator)
		h.addCondition(fmt.Sprintf("%s %s (%s)", alias, sqlOp, strings.Join(placeholders, ", ")))
		return nil
	}

	// Handle regular operators
	if sqlOp := h.getOperator(operator); sqlOp != "" {
		// Convert value to appropriate type
		paramValue := h.convertValue(value)
		h.params = append(h.params, paramValue)
		h.addCondition(fmt.Sprintf("%s %s $%d", alias, sqlOp, h.paramIndex))
		h.paramIndex++
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

	// If it's a NOT operator, wrap the condition in an AND group
	joinOp := group.operator
	if joinOp == opNOT {
		joinOp = opAND
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
	h.orderBy = append(h.orderBy, fmt.Sprintf("%s %s", alias, direction))
}

// GetWhereClause returns the final WHERE clause and parameters
func (h *SQLParseHook) GetWhereClause() (string, []any) {
	// Join all conditions with AND
	whereClause := strings.Join(h.conditions, fmt.Sprintf(" %s ", opAND))

	// Clean up any extra spaces
	whereClause = strings.TrimSpace(whereClause)

	return whereClause, h.params
}

// GetOrderByClause returns the final ORDER BY clause
func (h *SQLParseHook) GetOrderByClause() string {
	return strings.Join(h.orderBy, ", ")
}

// addCondition adds a condition to the current group or main conditions
func (h *SQLParseHook) addCondition(condition string) {
	if h.currentGroup != nil {
		h.currentGroup.operations = append(h.currentGroup.operations, condition)
	} else {
		h.conditions = append(h.conditions, condition)
	}
}

// getColumnAlias returns the qualified column name for the given field
func (h *SQLParseHook) getColumnAlias(field string, path []string) string {
	d := DefaultNameDelimiter
	if h.config != nil {
		d = h.config.nameDelimiter
	}

	field = fmt.Sprintf("%s%s%s", d, strings.TrimSpace(field), d)
	fullPath := strings.Join(path, "__")
	if fullPath != "" {
		return fmt.Sprintf("%s%s%s.%s", d, fullPath, d, field)
	}

	return field
}

// popLogicalGroup pops the current logical group from the stack
func (h *SQLParseHook) popLogicalGroup() *logicalGroup {
	if len(h.logicalStack) == 0 || h.currentGroup == nil || len(h.currentGroup.operations) == 0 {
		return nil
	}
	// Pop the current group from the stack
	pop := h.logicalStack[len(h.logicalStack)-1]
	h.logicalStack = h.logicalStack[:len(h.logicalStack)-1]

	// Update current group
	if len(h.logicalStack) > 0 {
		h.currentGroup = h.logicalStack[len(h.logicalStack)-1]
	} else {
		h.currentGroup = nil
	}

	return pop
}

// pushLogicalGroup pushes a new logical group onto the stack
func (h *SQLParseHook) pushLogicalGroup(operator logicalOperator) {
	group := &logicalGroup{
		operator:   operator,
		operations: make([]string, 0),
	}
	h.logicalStack = append(h.logicalStack, group)
	h.currentGroup = group
}

// convertValue converts gjson.Result to appropriate Go type
func (h *SQLParseHook) convertValue(value gjson.Result) any {
	if h.config != nil && h.config.convertValueFn != nil {
		return h.config.convertValueFn(value)
	}
	return DefaultConvertValueFn(value)
}

func (h *SQLParseHook) getOperator(op string) string {
	if h.config != nil && h.config.operatorMap != nil {
		return h.config.operatorMap[op]
	}
	return DefaultOperatorMap[op]
}
