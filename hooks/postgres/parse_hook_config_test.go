//go:build !integration
// +build !integration

package postgres

import (
	"testing"

	"github.com/jmag-ic/gosura/hooks/sql"
	"github.com/stretchr/testify/assert"
)

func TestPostgresAggregates(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		// STRING_AGG tests
		{
			Name:               "STRING_AGG with default separator",
			Filter:             `{"aggregate":{"string_agg":{"field":"username"}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `STRING_AGG("username", ',') AS string_agg_username`,
			Params:             []any{},
		},
		{
			Name:               "STRING_AGG with custom separator",
			Filter:             `{"aggregate":{"string_agg":{"field":"username","separator":", "}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `STRING_AGG("username", ', ') AS string_agg_username`,
			Params:             []any{},
		},
		{
			Name:               "STRING_AGG with ORDER BY",
			Filter:             `{"aggregate":{"string_agg":{"field":"username","separator":", ","order_by":"created_at","direction":"desc"}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `STRING_AGG("username", ', ' ORDER BY "created_at" DESC) AS string_agg_username`,
			Params:             []any{},
		},
		{
			Name:               "STRING_AGG with ORDER BY default ASC",
			Filter:             `{"aggregate":{"string_agg":{"field":"email","separator":";","order_by":"email"}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `STRING_AGG("email", ';' ORDER BY "email" ASC) AS string_agg_email`,
			Params:             []any{},
		},

		// PERCENTILE_CONT tests
		{
			Name:               "PERCENTILE_CONT basic",
			Filter:             `{"aggregate":{"percentile_cont":{"field":"salary","percentile":0.95}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY "salary" ASC) AS percentile_cont_salary`,
			Params:             []any{},
		},
		{
			Name:               "PERCENTILE_CONT with direction",
			Filter:             `{"aggregate":{"percentile_cont":{"field":"price","percentile":0.5,"direction":"desc"}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "price" DESC) AS percentile_cont_price`,
			Params:             []any{},
		},
		{
			Name:               "PERCENTILE_CONT with percentile=0 (min value)",
			Filter:             `{"aggregate":{"percentile_cont":{"field":"salary","percentile":0}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY "salary" ASC) AS percentile_cont_salary`,
			Params:             []any{},
		},
		{
			Name:               "PERCENTILE_CONT with percentile=1 (max value)",
			Filter:             `{"aggregate":{"percentile_cont":{"field":"salary","percentile":1}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY "salary" ASC) AS percentile_cont_salary`,
			Params:             []any{},
		},
		{
			Name:   "PERCENTILE_CONT invalid percentile > 1",
			Filter: `{"aggregate":{"percentile_cont":{"field":"salary","percentile":1.5}}}`,
			ValidateErr: func(err error) {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "percentile between 0 and 1")
			},
			Params: []any{},
		},
		{
			Name:   "PERCENTILE_CONT invalid percentile < 0",
			Filter: `{"aggregate":{"percentile_cont":{"field":"salary","percentile":-0.1}}}`,
			ValidateErr: func(err error) {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "percentile between 0 and 1")
			},
			Params: []any{},
		},

		// PERCENTILE_DISC tests
		{
			Name:               "PERCENTILE_DISC basic",
			Filter:             `{"aggregate":{"percentile_disc":{"field":"age","percentile":0.75}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY "age" ASC) AS percentile_disc_age`,
			Params:             []any{},
		},

		// ARRAY_AGG tests
		{
			Name:               "ARRAY_AGG basic",
			Filter:             `{"aggregate":{"array_agg":"tags"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `ARRAY_AGG("tags") AS array_agg_tags`,
			Params:             []any{},
		},
		{
			Name:               "ARRAY_AGG with DISTINCT",
			Filter:             `{"aggregate":{"array_agg":{"field":"category","distinct":true}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `ARRAY_AGG(DISTINCT "category") AS array_agg_category`,
			Params:             []any{},
		},
		{
			Name:               "ARRAY_AGG with ORDER BY",
			Filter:             `{"aggregate":{"array_agg":{"field":"product_name","order_by":"product_name","direction":"asc"}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `ARRAY_AGG("product_name" ORDER BY "product_name" ASC) AS array_agg_product_name`,
			Params:             []any{},
		},
		{
			Name:               "ARRAY_AGG with DISTINCT and ORDER BY",
			Filter:             `{"aggregate":{"array_agg":{"field":"status","distinct":true,"order_by":"status","direction":"desc"}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `ARRAY_AGG(DISTINCT "status" ORDER BY "status" DESC) AS array_agg_status`,
			Params:             []any{},
		},

		// JSON_AGG and JSONB_AGG tests
		{
			Name:               "JSON_AGG basic",
			Filter:             `{"aggregate":{"json_agg":"user_data"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `JSON_AGG("user_data") AS json_agg_user_data`,
			Params:             []any{},
		},
		{
			Name:               "JSONB_AGG with ORDER BY",
			Filter:             `{"aggregate":{"jsonb_agg":{"field":"metadata","order_by":"created_at"}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `JSONB_AGG("metadata" ORDER BY "created_at" ASC) AS jsonb_agg_metadata`,
			Params:             []any{},
		},

		// Standard aggregates should still work
		{
			Name:               "COUNT with PostgreSQL config",
			Filter:             `{"aggregate":{"count":"*"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `COUNT(*) AS count`,
			Params:             []any{},
		},
		{
			Name:               "SUM with PostgreSQL config",
			Filter:             `{"aggregate":{"sum":"amount"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `SUM("amount") AS sum_amount`,
			Params:             []any{},
		},

		// Mixed aggregates
		{
			Name:               "Mix of standard and PostgreSQL-specific aggregates",
			Filter:             `{"aggregate":{"count":"*","sum":"price","string_agg":{"field":"tags","separator":","}}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `COUNT(*) AS count, STRING_AGG("tags", ',') AS string_agg_tags, SUM("price") AS sum_price`,
			Params:             []any{},
		},
	}

	sql.RunTestCases(t, tests, func() sql.SQLFilter {
		return sql.NewSQLFilter(NewParseHookConfig())
	})
}

func TestPostgresStatisticalAggregates(t *testing.T) {
	tests := []sql.SQLParseTestCase{
		{
			Name:               "STDDEV_POP",
			Filter:             `{"aggregate":{"stddev_pop":"score"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `STDDEV_POP("score") AS stddev_pop_score`,
			Params:             []any{},
		},
		{
			Name:               "VAR_SAMP",
			Filter:             `{"aggregate":{"var_samp":"measurement"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `VAR_SAMP("measurement") AS var_samp_measurement`,
			Params:             []any{},
		},
		{
			Name:               "BOOL_AND",
			Filter:             `{"aggregate":{"bool_and":"is_active"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `BOOL_AND("is_active") AS bool_and_is_active`,
			Params:             []any{},
		},
		{
			Name:               "BOOL_OR",
			Filter:             `{"aggregate":{"bool_or":"has_access"}}`,
			ExpectedWhere:      "",
			ExpectedAggregates: `BOOL_OR("has_access") AS bool_or_has_access`,
			Params:             []any{},
		},
	}

	sql.RunTestCases(t, tests, func() sql.SQLFilter {
		return sql.NewSQLFilter(NewParseHookConfig())
	})
}
