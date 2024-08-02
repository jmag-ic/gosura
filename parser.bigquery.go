package gosura

// bqOperatorMap is the operator map for the hasura filter parser for BigQuery
var bqOperatorMap = map[string]sqlOperator{
	"_eq":      {format: "%s = %s"},
	"_neq":     {format: "%s != %s"},
	"_gt":      {format: "%s > %s"},
	"_lt":      {format: "%s < %s"},
	"_gte":     {format: "%s >= %s"},
	"_lte":     {format: "%s <= %s"},
	"_in":      {format: "%s IN (%s)"},
	"_nin":     {format: "%s NOT IN (%s)"},
	"_is_null": {formatFn: isNullFormatFn},
	"_like":    {format: "%s LIKE %s"},
	"_nlike":   {format: "%s NOT LIKE %s"},
	"_ilike":   {format: "LOWER(%s) LIKE LOWER(%s)"},
	"_nilike":  {format: "LOWER(%s) NOT LIKE LOWER(%s)"},
	"_regex":   {format: "REGEXP_CONTAINS(%s, %s)"},
	"_nregex":  {format: "NOT REGEXP_CONTAINS(%s, %s)"},
}

// NewPgParser returns a new parser builder for PostgreSQL
func NewBqParser() *parserBuilder {
	return newParser().
		SetOperatorMap(bqOperatorMap).
		SetColumnDelimiter("`").
		SetStringDelimiter(`"`)
}
