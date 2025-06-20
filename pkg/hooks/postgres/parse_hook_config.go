package postgres

import "github.com/jmag-ic/gosura/pkg/hooks/sql"

var postgresOperatorMap = map[string]string{
	// Basic comparisons
	"_eq":  "=",
	"_neq": "!=",
	"_gt":  ">",
	"_lt":  "<",
	"_gte": ">=",
	"_lte": "<=",

	// Null checks
	"_is_null": "IS NULL",

	// IN / NOT IN
	"_in":  "IN",
	"_nin": "NOT IN",

	// LIKE operators
	"_like":   "LIKE",
	"_nlike":  "NOT LIKE",
	"_ilike":  "ILIKE",
	"_nilike": "NOT ILIKE",

	// Regex
	"_similar":  "SIMILAR TO",
	"_nsimilar": "NOT SIMILAR TO",
	"_regex":    "~",
	"_nregex":   "!~",
	"_iregex":   "~*",
	"_niregex":  "!~*",

	// JSONB contains
	"_contains":     "@>",
	"_contained_in": "<@",
	"_has_key":      "?",
	"_has_keys_any": "?|",
	"_has_keys_all": "?&",

	// Geometry/geography (PostGIS)
	// "_st_contains":   "ST_Contains",
	// "_st_crosses":    "ST_Crosses",
	// "_st_equals":     "ST_Equals",
	// "_st_intersects": "ST_Intersects",
	// "_st_overlaps":   "ST_Overlaps",
	// "_st_touches":    "ST_Touches",
	// "_st_within":     "ST_Within",
	// "_st_d_within":   "ST_DWithin",

	// Network
	"_nt_contains":  ">>=",
	"_nt_contained": "<<=",
}

func NewParseHookConfig() *sql.ParseHookConfig {
	return &sql.ParseHookConfig{
		OperatorMap:    postgresOperatorMap,
		NameDelimiter:  sql.DefaultNameDelimiter,
		ConvertValueFn: sql.DefaultConvertValueFn,
	}
}
