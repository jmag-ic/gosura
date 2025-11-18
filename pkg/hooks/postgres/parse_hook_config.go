package postgres

import (
	"fmt"
	"strings"

	"github.com/jmag-ic/gosura/pkg/hooks/sql"
	"github.com/tidwall/gjson"
)

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

// PostgresAggregateBuilder handles PostgreSQL-specific aggregate syntax
func PostgresAggregateBuilder(
	function string,
	sqlFunction string,
	field string,
	options gjson.Result,
	getColumnAlias func(string, []string) string,
) (string, error) {
	// Handle PostgreSQL-specific aggregates with special syntax
	switch function {
	case "string_agg":
		return buildStringAgg(sqlFunction, field, options, getColumnAlias)

	case "percentile_cont", "percentile_disc":
		return buildPercentile(function, sqlFunction, field, options, getColumnAlias)

	case "array_agg", "json_agg", "jsonb_agg":
		return buildArrayAgg(sqlFunction, field, options, getColumnAlias)

	default:
		// Fall back to default builder for standard aggregates
		return sql.DefaultAggregateBuilder(function, sqlFunction, field, options, getColumnAlias)
	}
}

// buildStringAgg handles STRING_AGG with delimiter and optional ORDER BY
// Syntax: STRING_AGG(field, delimiter) or STRING_AGG(field, delimiter ORDER BY field)
func buildStringAgg(sqlFn string, field string, options gjson.Result, getColumnAlias func(string, []string) string) (string, error) {
	separator := options.Get("separator").String()
	if separator == "" {
		separator = "," // default delimiter
	}

	fieldAlias := getColumnAlias(field, []string{})
	resultAlias := fmt.Sprintf("string_agg_%s", strings.ReplaceAll(field, ".", "_"))

	expr := fmt.Sprintf("%s(%s, '%s'", sqlFn, fieldAlias, separator)

	// Handle ORDER BY within STRING_AGG
	if orderBy := options.Get("order_by").String(); orderBy != "" {
		direction := options.Get("direction").String()
		if direction == "" {
			direction = "ASC"
		}
		orderByAlias := getColumnAlias(orderBy, []string{})
		expr += fmt.Sprintf(" ORDER BY %s %s", orderByAlias, strings.ToUpper(direction))
	}

	expr += fmt.Sprintf(") AS %s", resultAlias)
	return expr, nil
}

// buildPercentile handles PERCENTILE_CONT and PERCENTILE_DISC with WITHIN GROUP
// Syntax: PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY field)
func buildPercentile(function, sqlFn, field string, options gjson.Result, getColumnAlias func(string, []string) string) (string, error) {
	percentile := options.Get("percentile").Float()
	if percentile < 0 || percentile > 1 {
		return "", fmt.Errorf("%s requires percentile between 0 and 1, got %f", function, percentile)
	}

	fieldAlias := getColumnAlias(field, []string{})
	resultAlias := fmt.Sprintf("%s_%s", function, strings.ReplaceAll(field, ".", "_"))

	direction := options.Get("direction").String()
	if direction == "" {
		direction = "ASC"
	}

	expr := fmt.Sprintf("%s(%g) WITHIN GROUP (ORDER BY %s %s) AS %s",
		sqlFn, percentile, fieldAlias, strings.ToUpper(direction), resultAlias)

	return expr, nil
}

// buildArrayAgg handles ARRAY_AGG, JSON_AGG, and JSONB_AGG with optional DISTINCT and ORDER BY
// Syntax: ARRAY_AGG([DISTINCT] field [ORDER BY field])
func buildArrayAgg(sqlFn string, field string, options gjson.Result, getColumnAlias func(string, []string) string) (string, error) {
	hasDistinct := options.Get("distinct").Bool()
	distinct := ""
	if hasDistinct {
		distinct = "DISTINCT "
	}

	fieldAlias := getColumnAlias(field, []string{})
	resultAlias := fmt.Sprintf("%s_%s", strings.ToLower(sqlFn), strings.ReplaceAll(field, ".", "_"))

	expr := fmt.Sprintf("%s(%s%s", sqlFn, distinct, fieldAlias)

	// Handle ORDER BY within array aggregate
	if orderBy := options.Get("order_by").String(); orderBy != "" {
		direction := options.Get("direction").String()
		if direction == "" {
			direction = "ASC"
		}
		orderByAlias := getColumnAlias(orderBy, []string{})
		expr += fmt.Sprintf(" ORDER BY %s %s", orderByAlias, strings.ToUpper(direction))
	}

	expr += fmt.Sprintf(") AS %s", resultAlias)
	return expr, nil
}

func NewParseHookConfig() *sql.ParseHookConfig {
	return &sql.ParseHookConfig{
		OperatorMap:        postgresOperatorMap,
		AggregateFnMap:     postgresAggregateFnMap,
		AggregateBuilderFn: PostgresAggregateBuilder,
		NameDelimiter:      sql.DefaultNameDelimiter,
		ConvertValueFn:     sql.DefaultConvertValueFn,
	}
}
