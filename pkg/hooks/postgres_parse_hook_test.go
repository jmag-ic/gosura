package hooks

import "testing"

func TestPostgresParseHook_PostgresOperatos(t *testing.T) {
	tests := []testCase{
		{
			name:          "_eq",
			filter:        `{"where":{"name":{"_eq":"John"}}}`,
			expectedWhere: `"name" = $1`,
			params:        []any{"John"},
		},
		{
			name:          "_neq",
			filter:        `{"where":{"name":{"_neq":"John"}}}`,
			expectedWhere: `"name" != $1`,
			params:        []any{"John"},
		},
		{
			name:          "_gt",
			filter:        `{"where":{"age":{"_gt":21}}}`,
			expectedWhere: `"age" > $1`,
			params:        []any{int64(21)},
		},
		{
			name:          "_lt",
			filter:        `{"where":{"age":{"_lt":21}}}`,
			expectedWhere: `"age" < $1`,
			params:        []any{int64(21)},
		},
		{
			name:          "_gte",
			filter:        `{"where":{"age":{"_gte":21}}}`,
			expectedWhere: `"age" >= $1`,
			params:        []any{int64(21)},
		},
		{
			name:          "_lte",
			filter:        `{"where":{"age":{"_lte":21}}}`,
			expectedWhere: `"age" <= $1`,
			params:        []any{int64(21)},
		},
		{
			name:          "_is_null",
			filter:        `{"where":{"name":{"_is_null":true}}}`,
			expectedWhere: `"name" IS NULL`,
			params:        []any{},
		},
		{
			name:          "_is_null false",
			filter:        `{"where":{"name":{"_is_null":false}}}`,
			expectedWhere: `"name" IS NOT NULL`,
			params:        []any{},
		},
		{
			name:          "_in",
			filter:        `{"where":{"age":{"_in":[21, 22, 23]}}}`,
			expectedWhere: `"age" IN ($1, $2, $3)`,
			params:        []any{int64(21), int64(22), int64(23)},
		},
		{
			name:          "_nin",
			filter:        `{"where":{"age":{"_nin":[21, 22, 23]}}}`,
			expectedWhere: `"age" NOT IN ($1, $2, $3)`,
			params:        []any{int64(21), int64(22), int64(23)},
		},
		{
			name:          "_like",
			filter:        `{"where":{"name":{"_like":"%John%"}}}`,
			expectedWhere: `"name" LIKE $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "_nlike",
			filter:        `{"where":{"name":{"_nlike":"%John%"}}}`,
			expectedWhere: `"name" NOT LIKE $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "_ilike",
			filter:        `{"where":{"name":{"_ilike":"%John%"}}}`,
			expectedWhere: `"name" ILIKE $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "_nilike",
			filter:        `{"where":{"name":{"_nilike":"%John%"}}}`,
			expectedWhere: `"name" NOT ILIKE $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "_similar",
			filter:        `{"where":{"name":{"_similar":"%John%"}}}`,
			expectedWhere: `"name" SIMILAR TO $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "_nsimilar",
			filter:        `{"where":{"name":{"_nsimilar":"%John%"}}}`,
			expectedWhere: `"name" NOT SIMILAR TO $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "_regex",
			filter:        `{"where":{"name":{"_regex":"%John%"}}}`,
			expectedWhere: `"name" ~ $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "_nregex",
			filter:        `{"where":{"name":{"_nregex":"%John%"}}}`,
			expectedWhere: `"name" !~ $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "_iregex",
			filter:        `{"where":{"name":{"_iregex":"%John%"}}}`,
			expectedWhere: `"name" ~* $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "_niregex",
			filter:        `{"where":{"name":{"_niregex":"%John%"}}}`,
			expectedWhere: `"name" !~* $1`,
			params:        []any{"%John%"},
		},
		{
			name:          "_contains",
			filter:        `{"where":{"metadata":{"_contains":{"role": "admin"}}}}`,
			expectedWhere: `"metadata" @> $1`,
			params:        []any{`{"role": "admin"}`},
		},
		{
			name:          "_contained_in",
			filter:        `{"where":{"metadata":{"_contained_in":{"role": "admin"}}}`,
			expectedWhere: `"metadata" <@ $1`,
			params:        []any{`{"role": "admin"}`},
		},
		{
			name:          "_has_key",
			filter:        `{"where":{"metadata":{"_has_key":"role"}}}`,
			expectedWhere: `"metadata" ? $1`,
			params:        []any{"role"},
		},
		{
			name:          "_has_keys_any",
			filter:        `{"where":{"metadata":{"_has_keys_any":["role", "age"]}}}`,
			expectedWhere: `"metadata" ?| $1`,
			params:        []any{[]any{"role", "age"}},
		},
		{
			name:          "_has_keys_all",
			filter:        `{"where":{"metadata":{"_has_keys_all":["role", "age"]}}}`,
			expectedWhere: `"metadata" ?& $1`,
			params:        []any{[]any{"role", "age"}},
		},
	}
	runTestCases(t, tests, func() *SQLParseHook {
		return NewSQLParseHook(NewPostgresParseHookConfig())
	})
}
