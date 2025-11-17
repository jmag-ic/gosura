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

var postgresAggregateFnMap = map[string]string{
	// Standard SQL aggregates
	"count": "COUNT",
	"sum":   "SUM",
	"avg":   "AVG",
	"max":   "MAX",
	"min":   "MIN",

	// Statistical aggregates
	"stddev":      "STDDEV",
	"stddev_pop":  "STDDEV_POP",
	"stddev_samp": "STDDEV_SAMP",
	"variance":    "VARIANCE",
	"var_pop":     "VAR_POP",
	"var_samp":    "VAR_SAMP",

	// Array and JSON aggregates
	"array_agg": "ARRAY_AGG",
	"json_agg":  "JSON_AGG",
	"jsonb_agg": "JSONB_AGG",

	// String aggregates
	"string_agg": "STRING_AGG",

	// Boolean aggregates
	"bool_and": "BOOL_AND",
	"bool_or":  "BOOL_OR",

	// Other PostgreSQL-specific aggregates
	"corr":            "CORR",
	"percentile_cont": "PERCENTILE_CONT",
	"percentile_disc": "PERCENTILE_DISC",
}

func NewParseHookConfig() *sql.ParseHookConfig {
	return &sql.ParseHookConfig{
		OperatorMap:    postgresOperatorMap,
		AggregateFnMap: postgresAggregateFnMap,
		NameDelimiter:  sql.DefaultNameDelimiter,
		ConvertValueFn: sql.DefaultConvertValueFn,
	}
}
