package gosura

import (
	"context"
	"fmt"
)

// parserBuilder is a builder for the hasura filter parser
type parserBuilder struct {
	parser *parser
}

// Extract adds fields to the parser's extractor map. These fields will be ignored by the parser and added to the Extracts field in the Filter struct
func (b *parserBuilder) Extract(fields ...string) *parserBuilder {
	for _, field := range fields {
		b.parser.extractorMap[field] = true
	}
	return b
}

// Model sets the table spec for the parser based on the given model. The table spec is used to validate the filter fields and to generate the SQL query
func (b *parserBuilder) Model(model any) *parserBuilder {
	return b.SetTableSpec(GetFromRegistry(model))
}

// Parse parses the given filter JSON and returns a Filter struct
func (b *parserBuilder) Parse(ctx context.Context, filterJSON *string) (*Filter, error) {
	return b.parser.Parse(ctx, filterJSON)
}

// SetColumnDelimiter sets the column delimiter for the parser
func (b *parserBuilder) SetColumnDelimiter(delimiter string) *parserBuilder {
	b.parser.columnDelimiter = delimiter
	return b
}

// SetComplexityLimit sets the deep level limit for the parser. The parser will return an error if the complexity of the filter exceeds this limit
func (b *parserBuilder) SetComplexityLimit(limit int) *parserBuilder {
	b.parser.complexityLimit = limit
	return b
}

// SetOperatorMap sets the operator map for the parser.
func (b *parserBuilder) SetOperatorMap(operatorMap map[string]sqlOperator) *parserBuilder {
	b.parser.operatorMap = operatorMap
	return b
}

// SetStringDelimiter sets the string delimiter for the parser
func (b *parserBuilder) SetStringDelimiter(delimiter string) *parserBuilder {
	b.parser.stringDelimiter = delimiter
	return b
}

// SetTableSpec sets the table spec for the parser. The table spec is used to validate the filter fields and to generate the SQL query
func (b *parserBuilder) SetTableSpec(tableSpec *TableSpec) *parserBuilder {
	b.parser.tableSpec = tableSpec
	return b
}

// newParser returns a new parser builder
func newParser() *parserBuilder {
	return &parserBuilder{
		parser: &parser{
			extractorMap:    map[string]bool{},
			complexityLimit: 3, // default complexity limit
		},
	}
}

// isNullFormatFn is a format function for the _is_null operator.
// It returns the correct format based on the value of the operator; "IS NULL" for true and "IS NOT NULL" for false.
var isNullFormatFn = func(value interface{}) (string, error) {
	isNull, ok := value.(bool)
	if !ok {
		return "", fmt.Errorf("expected boolean for _is_null operator, got %T", value)
	}
	if isNull {
		return "%s IS NULL", nil
	}
	return "%s IS NOT NULL", nil
}
