package gosura

// pgOperatorMap is the operator map for the hasura filter parser for PostgreSQL
var pgOperatorMap = map[string]sqlOperator{
	"_eq":       {format: "%s = %s"},
	"_neq":      {format: "%s != %s"},
	"_gt":       {format: "%s > %s"},
	"_lt":       {format: "%s < %s"},
	"_gte":      {format: "%s >= %s"},
	"_lte":      {format: "%s <= %s"},
	"_in":       {format: "%s IN (%s)"},
	"_nin":      {format: "%s NOT IN (%s)"},
	"_is_null":  {formatFn: isNullFormatFn},
	"_like":     {format: "%s LIKE %s"},
	"_nlike":    {format: "%s NOT LIKE %s"},
	"_ilike":    {format: "%s ILIKE %s"},
	"_nilike":   {format: "%s NOT ILIKE %s"},
	"_similar":  {format: "%s SIMILAR TO %s"},
	"_nsimilar": {format: "%s NOT SIMILAR TO %s"},
	"_regex":    {format: "%s ~ %s"},
	"_iregex":   {format: "%s ~* %s"},
	"_nregex":   {format: "%s !~ %s"},
	"_niregex":  {format: "%s !~* %s"},
}

// NewPgParser returns a new parser builder for PostgreSQL
func NewPgParser() *parserBuilder {
	return newParser().
		SetOperatorMap(pgOperatorMap).
		SetColumnDelimiter(`"`).
		SetStringDelimiter(`'`)
}
