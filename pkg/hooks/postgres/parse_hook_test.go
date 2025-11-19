//go:build !integration
// +build !integration

package postgres

import (
	"testing"

	"github.com/jmag-ic/gosura/pkg/hooks/sql"
)

func TestPostgresParseHook_PostgresOperators(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		{
			Name:          "_eq",
			Filter:        `{"where":{"name":{"_eq":"John"}}}`,
			ExpectedWhere: `"name" = $1`,
			Params:        []any{"John"},
		},
		{
			Name:          "_neq",
			Filter:        `{"where":{"name":{"_neq":"John"}}}`,
			ExpectedWhere: `"name" != $1`,
			Params:        []any{"John"},
		},
		{
			Name:          "_gt",
			Filter:        `{"where":{"age":{"_gt":21}}}`,
			ExpectedWhere: `"age" > $1`,
			Params:        []any{int64(21)},
		},
		{
			Name:          "_lt",
			Filter:        `{"where":{"age":{"_lt":21}}}`,
			ExpectedWhere: `"age" < $1`,
			Params:        []any{int64(21)},
		},
		{
			Name:          "_gte",
			Filter:        `{"where":{"age":{"_gte":21}}}`,
			ExpectedWhere: `"age" >= $1`,
			Params:        []any{int64(21)},
		},
		{
			Name:          "_lte",
			Filter:        `{"where":{"age":{"_lte":21}}}`,
			ExpectedWhere: `"age" <= $1`,
			Params:        []any{int64(21)},
		},
		{
			Name:          "_is_null",
			Filter:        `{"where":{"name":{"_is_null":true}}}`,
			ExpectedWhere: `"name" IS NULL`,
			Params:        []any{},
		},
		{
			Name:          "_is_null false",
			Filter:        `{"where":{"name":{"_is_null":false}}}`,
			ExpectedWhere: `"name" IS NOT NULL`,
			Params:        []any{},
		},
		{
			Name:          "_in",
			Filter:        `{"where":{"age":{"_in":[21, 22, 23]}}}`,
			ExpectedWhere: `"age" IN ($1, $2, $3)`,
			Params:        []any{int64(21), int64(22), int64(23)},
		},
		{
			Name:          "_nin",
			Filter:        `{"where":{"age":{"_nin":[21, 22, 23]}}}`,
			ExpectedWhere: `"age" NOT IN ($1, $2, $3)`,
			Params:        []any{int64(21), int64(22), int64(23)},
		},
		{
			Name:          "_like",
			Filter:        `{"where":{"name":{"_like":"%John%"}}}`,
			ExpectedWhere: `"name" LIKE $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "_nlike",
			Filter:        `{"where":{"name":{"_nlike":"%John%"}}}`,
			ExpectedWhere: `"name" NOT LIKE $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "_ilike",
			Filter:        `{"where":{"name":{"_ilike":"%John%"}}}`,
			ExpectedWhere: `"name" ILIKE $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "_nilike",
			Filter:        `{"where":{"name":{"_nilike":"%John%"}}}`,
			ExpectedWhere: `"name" NOT ILIKE $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "_similar",
			Filter:        `{"where":{"name":{"_similar":"%John%"}}}`,
			ExpectedWhere: `"name" SIMILAR TO $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "_nsimilar",
			Filter:        `{"where":{"name":{"_nsimilar":"%John%"}}}`,
			ExpectedWhere: `"name" NOT SIMILAR TO $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "_regex",
			Filter:        `{"where":{"name":{"_regex":"%John%"}}}`,
			ExpectedWhere: `"name" ~ $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "_nregex",
			Filter:        `{"where":{"name":{"_nregex":"%John%"}}}`,
			ExpectedWhere: `"name" !~ $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "_iregex",
			Filter:        `{"where":{"name":{"_iregex":"%John%"}}}`,
			ExpectedWhere: `"name" ~* $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "_niregex",
			Filter:        `{"where":{"name":{"_niregex":"%John%"}}}`,
			ExpectedWhere: `"name" !~* $1`,
			Params:        []any{"%John%"},
		},
		{
			Name:          "_contains",
			Filter:        `{"where":{"metadata":{"_contains":{"role": "admin"}}}}`,
			ExpectedWhere: `"metadata" @> $1`,
			Params:        []any{`{"role": "admin"}`},
		},
		{
			Name:          "_contained_in",
			Filter:        `{"where":{"metadata":{"_contained_in":{"role": "admin"}}}`,
			ExpectedWhere: `"metadata" <@ $1`,
			Params:        []any{`{"role": "admin"}`},
		},
		{
			Name:          "_has_key",
			Filter:        `{"where":{"metadata":{"_has_key":"role"}}}`,
			ExpectedWhere: `"metadata" ? $1`,
			Params:        []any{"role"},
		},
		{
			Name:          "_has_keys_any",
			Filter:        `{"where":{"metadata":{"_has_keys_any":["role", "age"]}}}`,
			ExpectedWhere: `"metadata" ?| $1`,
			Params:        []any{[]any{"role", "age"}},
		},
		{
			Name:          "_has_keys_all",
			Filter:        `{"where":{"metadata":{"_has_keys_all":["role", "age"]}}}`,
			ExpectedWhere: `"metadata" ?& $1`,
			Params:        []any{[]any{"role", "age"}},
		},
	}
	sql.RunTestCases(t, tests, func() *sql.SQLParseHook {
		return sql.NewSQLParseHook(NewParseHookConfig())
	})
}
