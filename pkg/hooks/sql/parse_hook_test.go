//go:build !integration
// +build !integration

package sql_test

import (
	"testing"

	"github.com/jmag-ic/gosura/pkg/hooks/sql"
	"github.com/stretchr/testify/assert"
)

func TestSQLParseHook_EmptyFilter(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		{
			Name:          "Empty filter string",
			Filter:        "",
			ExpectedWhere: "",
			Params:        []any{},
		},
		{
			Name:          "Empty filter",
			Filter:        `{}`,
			ExpectedWhere: "",
			Params:        []any{},
		},
		{
			Name:          "Empty _and",
			Filter:        `{"where":{"_and":[]}}`,
			ExpectedWhere: "",
			Params:        []any{},
		},
		{
			Name:          "Empty _or",
			Filter:        `{"where":{"_or":[]}}`,
			ExpectedWhere: "",
			Params:        []any{},
		},
		{
			Name:          "Empty _not",
			Filter:        `{"where":{"_not":{}}}`,
			ExpectedWhere: "",
			Params:        []any{},
		},
		{
			Name:          "Empty logical chain",
			Filter:        `{"where":{"_not":{"_and":[{"_or":[]}]}}}`,
			ExpectedWhere: "",
			Params:        []any{},
		},
	}

	sql.RunTestCases(t, tests, func() *sql.SQLParseHook {
		return sql.NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_LogicalGroups(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		{
			Name:          "Simple AND",
			Filter:        `{"where":{"_and":[{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}`,
			ExpectedWhere: `("age" > $1 AND "name" LIKE $2)`,
			Params:        []any{int64(18), "%John%"},
		},
		{
			Name:          "Simple OR",
			Filter:        `{"where":{"_or":[{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}`,
			ExpectedWhere: `("age" > $1 OR "name" LIKE $2)`,
			Params:        []any{int64(18), "%John%"},
		},
		{
			Name:          "Simple NOT",
			Filter:        `{"where":{"_not":{"age":{"_gt":18}}}}`,
			ExpectedWhere: `NOT "age" > $1`,
			Params:        []any{int64(18)},
		},
		{
			Name:          "Multiple fields in NOT",
			Filter:        `{"where":{"_not":{"age":{"_gt":18},"name":{"_like":"%John%"}}}}`,
			ExpectedWhere: `NOT ("age" > $1 AND "name" LIKE $2)`,
			Params:        []any{int64(18), "%John%"},
		},
		{
			Name:          "NOT with AND",
			Filter:        `{"where":{"_not":{"_and":[{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}}`,
			ExpectedWhere: `NOT ("age" > $1 AND "name" LIKE $2)`,
			Params:        []any{int64(18), "%John%"},
		},
		{
			Name:          "NOT with OR",
			Filter:        `{"where":{"_not":{"_or":[{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}}`,
			ExpectedWhere: `NOT ("age" > $1 OR "name" LIKE $2)`,
			Params:        []any{int64(18), "%John%"},
		},
		{
			Name:          "Unique condition inside a chain",
			Filter:        `{"where":{"_not":{"_and":[{"_or":[{"age":{"_gt":18}}]}]}}}`,
			ExpectedWhere: `NOT "age" > $1`,
			Params:        []any{int64(18)},
		},
		{
			Name:          "Complex nested conditions",
			Filter:        `{"where":{"_and":[{"_or":[{"age":{"_gt":18}},{"name":{"_eq":"John"}}]},{"_not":{"_and":[{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}}}`,
			ExpectedWhere: `(("age" > $1 OR "name" = $2) AND NOT ("age" > $3 AND "name" LIKE $4))`,
			Params:        []any{int64(18), "John", int64(18), "%John%"},
		},
		{
			Name:          "Syntactic sugar",
			Filter:        `{"where":{"name":"jose","_or":{"age":{"_gt":18},"_not":{"role":{"name":null}}},"_not":{"age":{"_gt":18},"role":"admin"}}}`,
			ExpectedWhere: `"name" = $1 AND ("age" > $2 OR NOT "role"."name" IS NULL) AND NOT ("age" > $3 AND "role" = $4)`,
			Params:        []any{"jose", int64(18), int64(18), "admin"},
		},
	}
	sql.RunTestCases(t, tests, func() *sql.SQLParseHook {
		return sql.NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_ComparisonOperators(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		{
			Name:          "Simple equality",
			Filter:        `{"where":{"age":{"_eq":18}}}`,
			ExpectedWhere: `"age" = $1`,
			Params:        []any{int64(18)},
		},
		{
			Name:          "Not equal",
			Filter:        `{"where":{"age":{"_neq":18}}}`,
			ExpectedWhere: `"age" != $1`,
			Params:        []any{int64(18)},
		},
		{
			Name:          "Greater than",
			Filter:        `{"where":{"age":{"_gt":18}}}`,
			ExpectedWhere: `"age" > $1`,
			Params:        []any{int64(18)},
		},
		{
			Name:          "Greater than or equal",
			Filter:        `{"where":{"age":{"_gte":18}}}`,
			ExpectedWhere: `"age" >= $1`,
			Params:        []any{int64(18)},
		},
		{
			Name:          "Less than",
			Filter:        `{"where":{"age":{"_lt":18}}}`,
			ExpectedWhere: `"age" < $1`,
			Params:        []any{int64(18)},
		},
		{
			Name:          "Less than or equal",
			Filter:        `{"where":{"age":{"_lte":18}}}`,
			ExpectedWhere: `"age" <= $1`,
			Params:        []any{int64(18)},
		},
		{
			Name:          "IN operator",
			Filter:        `{"where":{"age":{"_in":[18, 20, 22]}}}`,
			ExpectedWhere: `"age" IN ($1, $2, $3)`,
			Params:        []any{int64(18), int64(20), int64(22)},
		},
		{
			Name:          "NOT IN operator",
			Filter:        `{"where":{"age":{"_nin":[18, 20, 22]}}}`,
			ExpectedWhere: `"age" NOT IN ($1, $2, $3)`,
			Params:        []any{int64(18), int64(20), int64(22)},
		},
		{
			Name:          "LIKE operator",
			Filter:        `{"where":{"name":{"_like":"%John%"}}}`,
			ExpectedWhere: `"name" LIKE $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "ILIKE operator (case insensitive LIKE)",
			Filter:        `{"where":{"name":{"_ilike":"%John%"}}}`,
			ExpectedWhere: `"name" ILIKE $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "NOT LIKE operator",
			Filter:        `{"where":{"name":{"_nlike":"%John%"}}}`,
			ExpectedWhere: `"name" NOT LIKE $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "NOT ILIKE operator",
			Filter:        `{"where":{"name":{"_nilike":"%John%"}}}`,
			ExpectedWhere: `"name" NOT ILIKE $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "IS NULL operator",
			Filter:        `{"where":{"deleted_at":{"_is_null":true}}}`,
			ExpectedWhere: `"deleted_at" IS NULL`,
			Params:        []any{},
		},
		{
			Name:          "IS NULL operator with null value",
			Filter:        `{"where":{"deleted_at":null}}`,
			ExpectedWhere: `"deleted_at" IS NULL`,
			Params:        []any{},
		},
		{
			Name:          "IS NOT NULL operator",
			Filter:        `{"where":{"deleted_at":{"_is_null":false}}}`,
			ExpectedWhere: `"deleted_at" IS NOT NULL`,
			Params:        []any{},
		},
	}

	sql.RunTestCases(t, tests, func() *sql.SQLParseHook {
		return sql.NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_Associations(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		{
			Name:          "Simple association",
			Filter:        `{"where":{"user":{"name":{"_eq":"John"}}}}`,
			ExpectedWhere: `"user"."name" = $1`,
			Params:        []any{"John"},
		},
		{
			Name:          "Nested association",
			Filter:        `{"where":{"user":{"profile":{"name":{"_eq":"John"}}}}`,
			ExpectedWhere: `"user__profile"."name" = $1`,
			Params:        []any{"John"},
		},
		{
			Name:          "Multiple associations",
			Filter:        `{"where":{"user":{"name":{"_eq":"John"}},"role":{"permission":{"name":{"_eq":"admin"}}}}`,
			ExpectedWhere: `"user"."name" = $1 AND "role__permission"."name" = $2`,
			Params:        []any{"John", "admin"},
		},
	}

	sql.RunTestCases(t, tests, func() *sql.SQLParseHook {
		return sql.NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_OrderBy(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		{
			Name:            "Simple order by",
			Filter:          `{"where":{"age":{"_gt":18}}, "order_by":{"age": "asc"}}`,
			ExpectedWhere:   `"age" > $1`,
			Params:          []any{int64(18)},
			ExpectedOrderBy: `"age" ASC`,
		},
		{
			Name:            "Multiple order by",
			Filter:          `{"where":{"age":{"_gt":18}}, "order_by":{"age": "asc", "name": "desc"}}`,
			ExpectedWhere:   `"age" > $1`,
			Params:          []any{int64(18)},
			ExpectedOrderBy: `"age" ASC, "name" DESC`,
		},
		{
			Name:            "Multiple order by as array",
			Filter:          `{"where":{"age":{"_gt":18}}, "order_by":[{"age": "asc"}, {"name": "desc"}]}`,
			ExpectedWhere:   `"age" > $1`,
			Params:          []any{int64(18)},
			ExpectedOrderBy: `"age" ASC, "name" DESC`,
		},
		{
			Name:            "Nested order by",
			Filter:          `{"where":{"age":{"_gt":18}}, "order_by":{"user":{"name": "asc"}}}`,
			ExpectedWhere:   `"age" > $1`,
			Params:          []any{int64(18)},
			ExpectedOrderBy: `"user"."name" ASC`,
		},
		{
			Name:            "Multiple nested where and order by",
			Filter:          `{"where":{"user":{"age":{"_gt":18}}}, "order_by":{"user":{"name": "asc", "age": "desc"}}}`,
			ExpectedWhere:   `"user"."age" > $1`,
			Params:          []any{int64(18)},
			ExpectedOrderBy: `"user"."name" ASC, "user"."age" DESC`,
		},
	}
	sql.RunTestCases(t, tests, func() *sql.SQLParseHook {
		return sql.NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_Errors(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		{
			Name:          "Invalid operator",
			Filter:        `{"where":{"age":{"_invalid":18}}}`,
			ExpectedWhere: "",
			Params:        []any{},
			ValidateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "unsupported operator: _invalid", err.Error())
			},
		},
		{
			Name:          "Invalid array value with _in",
			Filter:        `{"where":{"age":{"_in": "invalid array"}}}`,
			ExpectedWhere: "",
			Params:        []any{},
			ValidateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "array value expected, got String", err.Error())
			},
		},
		{
			Name:          "Invalid array value with _nin",
			Filter:        `{"where":{"age":{"_nin": "invalid array"}}}`,
			ExpectedWhere: "",
			Params:        []any{},
			ValidateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "array value expected, got String", err.Error())
			},
		},
	}

	sql.RunTestCases(t, tests, func() *sql.SQLParseHook {
		return sql.NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_FullCoverage(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		{
			Name:            "simple equality and order",
			Filter:          `{"where":{"name":{"_eq":"John"}, "avg":7.5, "is_deleted":false, "is_admin":true},"order_by":{"age":"ASC"}}`,
			ExpectedWhere:   `"name" = $1 AND "avg" = $2 AND "is_deleted" = $3 AND "is_admin" = $4`,
			ExpectedOrderBy: `"age" ASC`,
			Params:          []any{"John", float64(7.5), false, true},
		},
		{
			Name:          "complex logical and comparison",
			Filter:        `{"where":{"_and":[{"age":{"_gt":21}},{"city": {"_in": ["NY", "LA"]}}]}}`,
			ExpectedWhere: `("age" > $1 AND "city" IN ($2, $3))`,
			Params:        []any{int64(21), "NY", "LA"},
		},
		{
			Name:          "null check",
			Filter:        `{"where": {"email": null}}`,
			ExpectedWhere: `"email" IS NULL`,
			Params:        []any{},
		},
		{
			Name:          "not and nested",
			Filter:        `{"where":{"_not":{"profile": {"age": {"_lt": 30}}}}}`,
			ExpectedWhere: `NOT "profile"."age" < $1`,
			Params:        []any{int64(30)},
		},
		{
			Name:   "invalid order direction",
			Filter: `{"order_by":{"name":"INVALID"}}`,
			ValidateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "invalid order_by direction: INVALID", err.Error())
			},
			Params: []any{},
		},
		{
			Name:   "empty key",
			Filter: `{"where": {"": {"_eq": "test"}}}`,
			ValidateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "empty key found in path: where", err.Error())
			},
			Params: []any{},
		},
		{
			Name:   "invalid filter structure",
			Filter: `{"where": "this should be an object"}`,
			ValidateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "invalid filter node: where", err.Error())
			},
			Params: []any{},
		},
		{
			Name:   "invalid order_by structure",
			Filter: `{"order_by": "ASC"}`,
			ValidateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "invalid order_by node: order_by", err.Error())
			},
			Params: []any{},
		},
	}

	sql.RunTestCases(t, tests, func() *sql.SQLParseHook {
		return &sql.SQLParseHook{
			Conditions:   make([]string, 0),
			Params:       make([]any, 0),
			ParamIndex:   1,
			LogicalStack: make([]*sql.LogicalGroup, 0),
			OrderBy:      make([]string, 0),
		}
	})
}

func TestSQLParseHook_Aggregates(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		{
			Name:               "Simple count",
			Filter:             `{"aggregate":{"count":"*"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `COUNT(*) AS count`,
			Params:             []any{},
		},
		{
			Name:               "Single field sum",
			Filter:             `{"aggregate":{"sum":"price"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `SUM("price") AS sum_price`,
			Params:             []any{},
		},
		{
			Name:               "Multiple fields in sum",
			Filter:             `{"aggregate":{"sum":["price","quantity"]}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `SUM("price") AS sum_price, SUM("quantity") AS sum_quantity`,
			Params:             []any{},
		},
		{
			Name:               "Multiple aggregate functions",
			Filter:             `{"aggregate":{"count":"*","avg":"rating","max":"price"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `COUNT(*) AS count, AVG("rating") AS avg_rating, MAX("price") AS max_price`,
			Params:             []any{},
		},
		{
			Name:               "Count with distinct",
			Filter:             `{"aggregate":{"count":{"field":"user_id","distinct":true}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `COUNT(DISTINCT "user_id") AS count_user_id`,
			Params:             []any{},
		},
		{
			Name:               "Aggregate with WHERE clause",
			Filter:             `{"where":{"status":{"_eq":"active"}},"aggregate":{"count":"*","avg":"rating"}}`,
			ExpectedWhere:      `"status" = $1`,
			ExpectedAggregates: `COUNT(*) AS count, AVG("rating") AS avg_rating`,
			Params:             []any{"active"},
		},
		{
			Name:               "Aggregate with ORDER BY",
			Filter:             `{"aggregate":{"count":"*","avg":"price"},"order_by":{"avg_price":"desc"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `COUNT(*) AS count, AVG("price") AS avg_price`,
			ExpectedOrderBy:    `"avg_price" DESC`,
			Params:             []any{},
		},
		{
			Name:               "All aggregate functions",
			Filter:             `{"aggregate":{"count":"*","sum":"price","avg":"rating","min":"stock","max":"created_at"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `COUNT(*) AS count, SUM("price") AS sum_price, AVG("rating") AS avg_rating, MIN("stock") AS min_stock, MAX("created_at") AS max_created_at`,
			Params:             []any{},
		},
		{
			Name:   "Unsupported aggregate function",
			Filter: `{"aggregate":{"unknown_func":"price"}}`,
			ValidateErr: func(err error) {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "unsupported aggregate function")
			},
			Params: []any{},
		},
	}

	sql.RunTestCases(t, tests, func() *sql.SQLParseHook {
		return sql.NewSQLParseHook(nil)
	})
}
