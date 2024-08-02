package gosura

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jmag-ic/gosura/internal/str"
)

var DefaultColumnPreloader ColumnPreloader

// ColumnPreloader is an interface for preloading columns from the context
type ColumnPreloader interface {
	Preloads(context.Context) []string
}

// sqlOperator represents an SQL operator
type sqlOperator struct {
	format   string
	formatFn func(value interface{}) (string, error)
}

// parser is the representation of a Hasura filter parser
type parser struct {
	complexityLimit int
	operatorMap     map[string]sqlOperator
	columnDelimiter string
	stringDelimiter string
	tableSpec       *TableSpec
	extractorMap    map[string]bool
	columnPreloader ColumnPreloader
}

// Parse parses a Hasura filter to a Filter structure
func (p *parser) Parse(ctx context.Context, filterJSON *string) (*Filter, error) {
	// Initialize the filter
	filter := &Filter{
		JoinsMap: make(map[string][]int),
		Extracts: make(map[string]interface{}),
	}

	// Set filter table and columns from the table spec if available
	if p.tableSpec != nil {
		// Set table name if table spec is provided
		filter.Table = p.tableSpec.Name

		// If column preloader is not set, use the default preloader
		if p.columnPreloader == nil {
			p.columnPreloader = DefaultColumnPreloader
		}

		// Add preloaded columns to the filter if column preloader is set
		if p.columnPreloader != nil {
			// Add requested columns to the filter
			for _, preload := range p.columnPreloader.Preloads(ctx) {
				column := p.tableColumn(preload, "", p.tableSpec)
				if column == "" {
					continue
				}
				filter.Columns = append(filter.Columns, column)
			}
		}
	}

	// If filterJSON is nil, return the empty filter
	if filterJSON == nil {
		return filter, nil
	}

	// Parse the filter JSON
	var filterInput FilterInput
	err := json.Unmarshal([]byte(*filterJSON), &filterInput)
	if err != nil {
		return nil, err
	}

	// Set distinct on columns if available
	if len(filterInput.DistinctOn) > 0 {
		for _, column := range filterInput.DistinctOn {
			filter.DistinctOn = append(filter.DistinctOn, p.tableColumn(column, "", p.tableSpec))
		}
	}

	// Set where clause if available
	if filterInput.Where != nil {
		filter.Where, err = p.parseConditions(filterInput.Where, filter, "", filter.Table, p.tableSpec)
		if err != nil {
			return nil, err
		}
	}

	// Reset complexity after parsing the where clause
	filter.complexity = 0

	// Set order by clause if available
	if filterInput.OrderBy != nil {
		filter.OrderBy, err = p.parseOrderBy(filterInput.OrderBy, filter, filter.Table, p.tableSpec)
		if err != nil {
			return nil, err
		}
	}

	// Set limit and offset clauses
	filter.Limit = filterInput.Limit
	filter.Offset = filterInput.Offset

	return filter, nil
}

// parseConditions parses a set of conditions and combines them with logical operators
func (p *parser) parseConditions(filterMap map[string]interface{}, filter *Filter, fieldPrefix, tableAlias string, tableSpec *TableSpec) (string, error) {
	filter.complexity++
	if filter.complexity > p.complexityLimit {
		return "", fmt.Errorf("filter complexity is too high")
	}

	var conditions []string

	for field, condition := range filterMap {
		switch field {
		case "_and", "_or", "_not":
			logicalCondition, err := p.parseLogicalOperator(field, condition, filter, fieldPrefix, tableAlias, tableSpec)
			if err != nil {
				return "", err
			}
			conditions = append(conditions, logicalCondition)
		default:
			// fullField is the field name considering its ancestors in the nested structure. i.e author.profile.firstTame
			fullField := fieldPrefix + field

			// If the field is registered in the extractor, then the parsed condition will be added to the extracts map and will be ignored in the where clause
			if p.extractorMap[fullField] {
				filter.Extracts[fullField] = condition
				continue
			}

			// If a join spec was defined for the field, add the join and parse the nested conditions
			if joinSpec := getJoinSpec(field, tableSpec); joinSpec != nil {
				// Add the join spec to the filter and get the nested table alias
				nestedTableAlias := filter.addJoinSpec(tableAlias, field, joinSpec)
				// Define the prefix for the nested fields full names
				nestedFieldPrefix := fullField + "."

				// Parse the nested conditions
				nestedConditions, err := p.parseConditions(condition.(map[string]interface{}), filter, nestedFieldPrefix, nestedTableAlias, joinSpec.TableSpec)
				if err != nil {
					return "", err
				}
				conditions = append(conditions, nestedConditions)
			} else {
				// Parse the conditions for the column
				columnConditions, err := p.parseColumnConditions(field, condition, filter, tableAlias, tableSpec)
				if err != nil {
					return "", err
				}
				conditions = append(conditions, columnConditions)
			}
		}
	}

	// Build the condition statement joining the conditions with an AND operator
	return strings.Join(conditions, " AND "), nil
}

// parseColumnConditions parses conditions for a specific column
func (p *parser) parseColumnConditions(column string, condition interface{}, filter *Filter, tableAlias string, tableSpec *TableSpec) (string, error) {
	switch cond := condition.(type) {
	case map[string]interface{}:
		var conditions []string
		for operator, value := range cond {
			sqlCondition, err := p.parseCondition(column, operator, value, filter, tableAlias, tableSpec)
			if err != nil {
				return "", err
			}
			conditions = append(conditions, sqlCondition)
		}
		return strings.Join(conditions, " AND "), nil
	default:
		return "", fmt.Errorf("unexpected condition type for column %s", column)
	}
}

// parseCondition parses a single condition and converts it to a SQL clause
func (p *parser) parseCondition(field, operator string, value interface{}, filter *Filter, tableAlias string, tableSpec *TableSpec) (string, error) {
	sqlOp, ok := p.operatorMap[operator]
	if !ok {
		return "", fmt.Errorf("unsupported operator %s", operator)
	}

	column := p.tableColumn(field, tableAlias, tableSpec)
	if column == "" {
		return "", fmt.Errorf("field '%s' not found in table spec", field)
	}

	// _is_null operator for association columns needs left joining instead of join
	if operator == "_is_null" && tableAlias != "" {
		filter.joinToLeftJoin(tableAlias)
	}

	var err error
	format := sqlOp.format

	if sqlOp.formatFn != nil {
		format, err = sqlOp.formatFn(value)
	}

	if err != nil {
		return "", err
	}

	return str.Sprintf(format, column, p.formatValue(value)), nil
}

// parseLogicalOperator parses logical operators and combines sub-conditions
func (p *parser) parseLogicalOperator(operator string, condition interface{}, filter *Filter, fieldPrefix, tableAlias string, tableSpec *TableSpec) (string, error) {
	subFilters, ok := condition.([]interface{})
	if !ok {
		return "", fmt.Errorf("expected array for logical operator %s, got %T", operator, condition)
	}

	var subConditions []string
	for _, subFilter := range subFilters {
		subFilterMap, ok := subFilter.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("expected map for sub-condition, got %T", subFilter)
		}
		parsedCondition, err := p.parseConditions(subFilterMap, filter, fieldPrefix, tableAlias, tableSpec)
		if err != nil {
			return "", err
		}
		if parsedCondition == "" {
			continue
		}
		subConditions = append(subConditions, parsedCondition)
	}

	switch operator {
	case "_and":
		return fmt.Sprintf("(%s)", strings.Join(subConditions, " AND ")), nil
	case "_or":
		return fmt.Sprintf("(%s)", strings.Join(subConditions, " OR ")), nil
	case "_not":
		if len(subConditions) != 1 {
			return "", fmt.Errorf("_not operator requires exactly one sub-condition")
		}
		return fmt.Sprintf("NOT (%s)", subConditions[0]), nil
	default:
		return "", fmt.Errorf("unsupported logical operator %s", operator)
	}
}

// parseOrderBy parses the orderBy clause
func (p *parser) parseOrderBy(orderBy interface{}, filter *Filter, tableAlias string, tableSpec *TableSpec) (string, error) {
	orderByClauses, err := p.getOrderByClauses(orderBy, filter, tableAlias, tableSpec)
	if err != nil {
		return "", err
	}

	return strings.Join(orderByClauses, ", "), nil
}

// getOrderByClauses parses the orderBy clause and returns a slice of order by clauses
func (p *parser) getOrderByClauses(orderBy interface{}, filter *Filter, tableAlias string, tableSpec *TableSpec) ([]string, error) {
	filter.complexity++
	if filter.complexity > p.complexityLimit {
		return nil, fmt.Errorf("filter complexity is too high")
	}

	switch ob := orderBy.(type) {
	case map[string]interface{}:
		return p.parseOrderByEntry(ob, filter, tableAlias, tableSpec)

	case []interface{}:
		var orderByClauses []string

		for _, entry := range ob {
			switch entry := entry.(type) {
			case map[string]interface{}:
				c, err := p.parseOrderByEntry(entry, filter, tableAlias, tableSpec)
				if err != nil {
					return nil, err
				}
				orderByClauses = append(orderByClauses, c...)

			case string:
				column := p.tableColumn(entry, tableAlias, tableSpec)
				if column == "" {
					return nil, fmt.Errorf("field '%s' not found in table spec", entry)
				}
				orderByClauses = append(orderByClauses, fmt.Sprintf("%s ASC", column))

			default:
				return nil, fmt.Errorf("unexpected type for orderBy entry: %T", entry)
			}
		}
		return orderByClauses, nil

	default:
		return nil, fmt.Errorf("unexpected type for orderBy: %T", orderBy)
	}
}

// parseOrderByEntry parses a single entry in the orderBy clause
func (p *parser) parseOrderByEntry(entry map[string]interface{}, filter *Filter, tableAlias string, tableSpec *TableSpec) ([]string, error) {
	var orderByClauses []string
	for field, value := range entry {
		if joinSpec := getJoinSpec(field, tableSpec); joinSpec != nil {
			nestedTableAlias := filter.addJoinSpec(tableAlias, field, joinSpec)
			nestedOrderByClauses, err := p.getOrderByClauses(value, filter, nestedTableAlias, joinSpec.TableSpec)
			if err != nil {
				return nil, err
			}
			orderByClauses = append(orderByClauses, nestedOrderByClauses...)
			continue
		}
		if dir, ok := value.(string); ok {
			value = strings.ToUpper(dir)
		}
		column := p.tableColumn(field, tableAlias, tableSpec)
		if column == "" {
			return nil, fmt.Errorf("field '%s' not found in table spec", field)
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf("%s %s", column, value))
	}
	return orderByClauses, nil
}

// formatValue formats a value for SQL based on its type
func (p *parser) formatValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return p.strVal(v)
	case int, int8, int16, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case []interface{}:
		var inValues []string
		for _, val := range v {
			inValues = append(inValues, p.formatValue(val))
		}
		return strings.Join(inValues, ",")
	default:
		return p.strVal(fmt.Sprintf("%v", v))
	}
}

// table returns a table name with the column delimiter
func (p *parser) table(alias string, tableSpec *TableSpec) string {
	if alias == "" && tableSpec != nil && tableSpec.Name != "" {
		alias = tableSpec.Name
	}

	if alias == "" {
		return ""
	}

	return fmt.Sprintf("%s%s%s", p.columnDelimiter, alias, p.columnDelimiter)
}

// column returns a column name with the column delimiter
func (p *parser) column(field string, tableSpec *TableSpec) string {
	var column string

	if tableSpec == nil {
		column = field
	} else {
		column = tableSpec.ColumnsMap[field]
		if column == "" {
			return ""
		}
	}
	return fmt.Sprintf("%s%s%s", p.columnDelimiter, column, p.columnDelimiter)
}

// tableColumn returns a column name with the column delimiter and table name
func (p *parser) tableColumn(field string, tableAlias string, tableSpec *TableSpec) string {
	table := p.table(tableAlias, tableSpec)
	if table != "" {
		table += "."
	}

	column := p.column(field, tableSpec)
	if column == "" {
		return ""
	}

	return fmt.Sprintf("%s%s", table, column)
}

// strVal returns a string value with the string delimiter
func (p *parser) strVal(value string) string {
	return fmt.Sprintf("%s%s%s", p.stringDelimiter, value, p.stringDelimiter)
}

// getJoinSpec returns the join specification for a given key
func getJoinSpec(key string, tableSpec *TableSpec) *JoinSpec {
	var joinSpec *JoinSpec
	if tableSpec != nil {
		joinSpec = tableSpec.JoinsMap[key]
	}

	return joinSpec
}
