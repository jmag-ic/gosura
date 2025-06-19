package hooks

import (
	"context"
	"testing"

	"github.com/jmag-ic/gosura/pkg/inspector"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name            string
	filter          string
	expectedWhere   string
	expectedOrderBy string
	params          []any
	validateErr     func(error)
}

func runTestCases(t *testing.T, tests []testCase, newHookFn func() *SQLParseHook) {
	hasuraInspector := &inspector.HasuraInspector{}
	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseHook := newHookFn()
			err := hasuraInspector.Inspect(ctx, tt.filter, parseHook)
			if tt.validateErr != nil {
				tt.validateErr(err)
			} else {
				assert.NoError(t, err)
			}

			whereClause, params := parseHook.GetWhereClause()
			assert.Equal(t, tt.expectedWhere, whereClause)
			assert.Equal(t, tt.params, params)

			orderByClause := parseHook.GetOrderByClause()
			assert.Equal(t, tt.expectedOrderBy, orderByClause)
		})
	}
}

func TestSQLParseHook_EmptyFilter(t *testing.T) {
	tests := []testCase{
		{
			name:          "Empty filter string",
			filter:        "",
			expectedWhere: "",
			params:        []any{},
		},
		{
			name:          "Empty filter",
			filter:        `{}`,
			expectedWhere: "",
			params:        []any{},
		},
		{
			name:          "Empty _and",
			filter:        `{"where":{"_and":[]}}`,
			expectedWhere: "",
			params:        []any{},
		},
		{
			name:          "Empty _or",
			filter:        `{"where":{"_or":[]}}`,
			expectedWhere: "",
			params:        []any{},
		},
		{
			name:          "Empty _not",
			filter:        `{"where":{"_not":{}}}`,
			expectedWhere: "",
			params:        []any{},
		},
		{
			name:          "Empty logical chain",
			filter:        `{"where":{"_not":{"_and":[{"_or":[]}]}}}`,
			expectedWhere: "",
			params:        []any{},
		},
	}

	runTestCases(t, tests, func() *SQLParseHook {
		return NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_LogicalGroups(t *testing.T) {
	tests := []testCase{
		{
			name:          "Simple AND",
			filter:        `{"where":{"_and":[{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}`,
			expectedWhere: `("age" > $1 AND "name" LIKE $2)`,
			params:        []any{int64(18), "%John%"},
		},
		{
			name:          "Simple OR",
			filter:        `{"where":{"_or":[{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}`,
			expectedWhere: `("age" > $1 OR "name" LIKE $2)`,
			params:        []any{int64(18), "%John%"},
		},
		{
			name:          "Simple NOT",
			filter:        `{"where":{"_not":{"age":{"_gt":18}}}}`,
			expectedWhere: `NOT "age" > $1`,
			params:        []any{int64(18)},
		},
		{
			name:          "Multiple fields in NOT",
			filter:        `{"where":{"_not":{"age":{"_gt":18},"name":{"_like":"%John%"}}}}`,
			expectedWhere: `NOT ("age" > $1 AND "name" LIKE $2)`,
			params:        []any{int64(18), "%John%"},
		},
		{
			name:          "NOT with AND",
			filter:        `{"where":{"_not":{"_and":[{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}}`,
			expectedWhere: `NOT ("age" > $1 AND "name" LIKE $2)`,
			params:        []any{int64(18), "%John%"},
		},
		{
			name:          "NOT with OR",
			filter:        `{"where":{"_not":{"_or":[{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}}`,
			expectedWhere: `NOT ("age" > $1 OR "name" LIKE $2)`,
			params:        []any{int64(18), "%John%"},
		},
		{
			name:          "Unique condition inside a chain",
			filter:        `{"where":{"_not":{"_and":[{"_or":[{"age":{"_gt":18}}]}]}}}`,
			expectedWhere: `NOT "age" > $1`,
			params:        []any{int64(18)},
		},
		{
			name:          "Complex nested conditions",
			filter:        `{"where":{"_and":[{"_or":[{"age":{"_gt":18}},{"name":{"_eq":"John"}}]},{"_not":{"_and":[{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}}}`,
			expectedWhere: `(("age" > $1 OR "name" = $2) AND NOT ("age" > $3 AND "name" LIKE $4))`,
			params:        []any{int64(18), "John", int64(18), "%John%"},
		},
		{
			name:          "Syntactic sugar",
			filter:        `{"where":{"name":"jose","_or":{"age":{"_gt":18},"_not":{"role":{"name":null}}},"_not":{"age":{"_gt":18},"role":"admin"}}}`,
			expectedWhere: `"name" = $1 AND ("age" > $2 OR NOT "role"."name" IS NULL) AND NOT ("age" > $3 AND "role" = $4)`,
			params:        []any{"jose", int64(18), int64(18), "admin"},
		},
	}
	runTestCases(t, tests, func() *SQLParseHook {
		return NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_ComparisonOperators(t *testing.T) {
	tests := []testCase{
		{
			name:          "Simple equality",
			filter:        `{"where":{"age":{"_eq":18}}}`,
			expectedWhere: `"age" = $1`,
			params:        []any{int64(18)},
		},
		{
			name:          "Not equal",
			filter:        `{"where":{"age":{"_neq":18}}}`,
			expectedWhere: `"age" != $1`,
			params:        []any{int64(18)},
		},
		{
			name:          "Greater than",
			filter:        `{"where":{"age":{"_gt":18}}}`,
			expectedWhere: `"age" > $1`,
			params:        []any{int64(18)},
		},
		{
			name:          "Greater than or equal",
			filter:        `{"where":{"age":{"_gte":18}}}`,
			expectedWhere: `"age" >= $1`,
			params:        []any{int64(18)},
		},
		{
			name:          "Less than",
			filter:        `{"where":{"age":{"_lt":18}}}`,
			expectedWhere: `"age" < $1`,
			params:        []any{int64(18)},
		},
		{
			name:          "Less than or equal",
			filter:        `{"where":{"age":{"_lte":18}}}`,
			expectedWhere: `"age" <= $1`,
			params:        []any{int64(18)},
		},
		{
			name:          "IN operator",
			filter:        `{"where":{"age":{"_in":[18, 20, 22]}}}`,
			expectedWhere: `"age" IN ($1, $2, $3)`,
			params:        []any{int64(18), int64(20), int64(22)},
		},
		{
			name:          "NOT IN operator",
			filter:        `{"where":{"age":{"_nin":[18, 20, 22]}}}`,
			expectedWhere: `"age" NOT IN ($1, $2, $3)`,
			params:        []any{int64(18), int64(20), int64(22)},
		},
		{
			name:          "LIKE operator",
			filter:        `{"where":{"name":{"_like":"%John%"}}}`,
			expectedWhere: `"name" LIKE $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "ILIKE operator (case insensitive LIKE)",
			filter:        `{"where":{"name":{"_ilike":"%John%"}}}`,
			expectedWhere: `"name" ILIKE $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "NOT LIKE operator",
			filter:        `{"where":{"name":{"_nlike":"%John%"}}}`,
			expectedWhere: `"name" NOT LIKE $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "NOT ILIKE operator",
			filter:        `{"where":{"name":{"_nilike":"%John%"}}}`,
			expectedWhere: `"name" NOT ILIKE $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "IS NULL operator",
			filter:        `{"where":{"deleted_at":{"_is_null":true}}}`,
			expectedWhere: `"deleted_at" IS NULL`,
			params:        []any{},
		},
		{
			name:          "IS NULL operator with null value",
			filter:        `{"where":{"deleted_at":null}}`,
			expectedWhere: `"deleted_at" IS NULL`,
			params:        []any{},
		},
		{
			name:          "IS NOT NULL operator",
			filter:        `{"where":{"deleted_at":{"_is_null":false}}}`,
			expectedWhere: `"deleted_at" IS NOT NULL`,
			params:        []any{},
		},
	}

	runTestCases(t, tests, func() *SQLParseHook {
		return NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_Associations(t *testing.T) {
	tests := []testCase{
		{
			name:          "Simple association",
			filter:        `{"where":{"user":{"name":{"_eq":"John"}}}}`,
			expectedWhere: `"user"."name" = $1`,
			params:        []any{"John"},
		},
		{
			name:          "Nested association",
			filter:        `{"where":{"user":{"profile":{"name":{"_eq":"John"}}}}`,
			expectedWhere: `"user__profile"."name" = $1`,
			params:        []any{"John"},
		},
		{
			name:          "Multiple associations",
			filter:        `{"where":{"user":{"name":{"_eq":"John"}},"role":{"permission":{"name":{"_eq":"admin"}}}}`,
			expectedWhere: `"user"."name" = $1 AND "role__permission"."name" = $2`,
			params:        []any{"John", "admin"},
		},
	}

	runTestCases(t, tests, func() *SQLParseHook {
		return NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_OrderBy(t *testing.T) {
	tests := []testCase{
		{
			name:            "Simple order by",
			filter:          `{"where":{"age":{"_gt":18}}, "order_by":{"age": "asc"}}`,
			expectedWhere:   `"age" > $1`,
			params:          []any{int64(18)},
			expectedOrderBy: `"age" ASC`,
		},
		{
			name:            "Multiple order by",
			filter:          `{"where":{"age":{"_gt":18}}, "order_by":{"age": "asc", "name": "desc"}}`,
			expectedWhere:   `"age" > $1`,
			params:          []any{int64(18)},
			expectedOrderBy: `"age" ASC, "name" DESC`,
		},
		{
			name:            "Multiple order by as array",
			filter:          `{"where":{"age":{"_gt":18}}, "order_by":[{"age": "asc"}, {"name": "desc"}]}`,
			expectedWhere:   `"age" > $1`,
			params:          []any{int64(18)},
			expectedOrderBy: `"age" ASC, "name" DESC`,
		},
		{
			name:            "Nested order by",
			filter:          `{"where":{"age":{"_gt":18}}, "order_by":{"user":{"name": "asc"}}}`,
			expectedWhere:   `"age" > $1`,
			params:          []any{int64(18)},
			expectedOrderBy: `"user"."name" ASC`,
		},
		{
			name:            "Multiple nested where and order by",
			filter:          `{"where":{"user":{"age":{"_gt":18}}}, "order_by":{"user":{"name": "asc", "age": "desc"}}}`,
			expectedWhere:   `"user"."age" > $1`,
			params:          []any{int64(18)},
			expectedOrderBy: `"user"."name" ASC, "user"."age" DESC`,
		},
	}
	runTestCases(t, tests, func() *SQLParseHook {
		return NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_Errors(t *testing.T) {
	tests := []testCase{
		{
			name:          "Invalid operator",
			filter:        `{"where":{"age":{"_invalid":18}}}`,
			expectedWhere: "",
			params:        []any{},
			validateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "unsupported operator: _invalid", err.Error())
			},
		},
		{
			name:          "Invalid array value with _in",
			filter:        `{"where":{"age":{"_in": "invalid array"}}}`,
			expectedWhere: "",
			params:        []any{},
			validateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "array value expected, got String", err.Error())
			},
		},
		{
			name:          "Invalid array value with _nin",
			filter:        `{"where":{"age":{"_nin": "invalid array"}}}`,
			expectedWhere: "",
			params:        []any{},
			validateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "array value expected, got String", err.Error())
			},
		},
	}

	runTestCases(t, tests, func() *SQLParseHook {
		return NewSQLParseHook(nil)
	})
}

func TestSQLParseHook_FullCoverage(t *testing.T) {
	tests := []testCase{
		{
			name:            "simple equality and order",
			filter:          `{"where":{"name":{"_eq":"John"}, "avg":7.5, "is_deleted":false, "is_admin":true},"order_by":{"age":"ASC"}}`,
			expectedWhere:   `"name" = $1 AND "avg" = $2 AND "is_deleted" = $3 AND "is_admin" = $4`,
			expectedOrderBy: `"age" ASC`,
			params:          []any{"John", float64(7.5), false, true},
		},
		{
			name:          "complex logical and comparison",
			filter:        `{"where":{"_and":[{"age":{"_gt":21}},{"city": {"_in": ["NY", "LA"]}}]}}`,
			expectedWhere: `("age" > $1 AND "city" IN ($2, $3))`,
			params:        []any{int64(21), "NY", "LA"},
		},
		{
			name:          "null check",
			filter:        `{"where": {"email": null}}`,
			expectedWhere: `"email" IS NULL`,
			params:        []any{},
		},
		{
			name:          "not and nested",
			filter:        `{"where":{"_not":{"profile": {"age": {"_lt": 30}}}}}`,
			expectedWhere: `NOT "profile"."age" < $1`,
			params:        []any{int64(30)},
		},
		{
			name:   "invalid order direction",
			filter: `{"order_by":{"name":"INVALID"}}`,
			validateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "invalid order_by direction: INVALID", err.Error())
			},
			params: []any{},
		},
		{
			name:   "empty key",
			filter: `{"where": {"": {"_eq": "test"}}}`,
			validateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "empty key found in path: where", err.Error())
			},
			params: []any{},
		},
		{
			name:   "invalid filter structure",
			filter: `{"where": "this should be an object"}`,
			validateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "invalid filter node: where", err.Error())
			},
			params: []any{},
		},
		{
			name:   "invalid order_by structure",
			filter: `{"order_by": "ASC"}`,
			validateErr: func(err error) {
				assert.Error(t, err)
				assert.Equal(t, "invalid order_by node: order_by", err.Error())
			},
			params: []any{},
		},
	}

	runTestCases(t, tests, func() *SQLParseHook {
		return &SQLParseHook{
			conditions:   make([]string, 0),
			params:       make([]any, 0),
			paramIndex:   1,
			logicalStack: make([]*logicalGroup, 0),
			orderBy:      make([]string, 0),
		}
	})
}
