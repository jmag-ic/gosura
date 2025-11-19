package sql

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/jmag-ic/gosura/pkg/inspector"
	"github.com/stretchr/testify/assert"
)

type SQLParseTestCase struct {
	Name               string
	Filter             string
	ExpectedWhere      string
	ExpectedOrderBy    string
	ExpectedAggregates string
	Params             []any
	ValidateErr        func(error)
	ValidateHook       func(*SQLParseHook)
}

func RunTestCases(t *testing.T, tests []SQLParseTestCase, newHookFn func() *SQLParseHook) {
	hasuraInspector := &inspector.HasuraInspector{}
	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			parseHook := newHookFn()

			err := hasuraInspector.Inspect(ctx, tt.Filter, parseHook)
			if tt.ValidateErr != nil {
				tt.ValidateErr(err)
			} else {
				assert.NoError(t, err)
			}

			qb := parseHook.GetQueryBuilder()

			// Validate WHERE clause
			whereClause := strings.Join(qb.Conditions, " AND ")
			assert.Equal(t, tt.ExpectedWhere, whereClause)
			assert.Equal(t, tt.Params, qb.Params)

			// Validate ORDER BY clause
			orderByClause := strings.Join(qb.OrderBy, ", ")
			assert.Equal(t, tt.ExpectedOrderBy, orderByClause)

			// Validate aggregates
			if tt.ExpectedAggregates != "" {
				// Build aggregates string from map
				aggregateStrs := make([]string, 0, len(qb.Aggregates))
				for alias, expr := range qb.Aggregates {
					aggregateStrs = append(aggregateStrs, expr+" AS "+alias)
				}
				// Sort for consistent comparison
				sort.Strings(aggregateStrs)
				aggregatesStr := strings.Join(aggregateStrs, ", ")
				assert.Equal(t, tt.ExpectedAggregates, aggregatesStr)
			}

			// Custom hook validation
			if tt.ValidateHook != nil {
				tt.ValidateHook(parseHook)
			}
		})
	}
}
