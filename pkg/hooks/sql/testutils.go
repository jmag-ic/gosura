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

			whereClause, params := parseHook.GetWhereClause()
			assert.Equal(t, tt.ExpectedWhere, whereClause)
			assert.Equal(t, tt.Params, params)

			orderByClause := parseHook.GetOrderByClause()
			assert.Equal(t, tt.ExpectedOrderBy, orderByClause)

			if tt.ExpectedAggregates != "" {
				aggregates := parseHook.GetAggregates()

				// Make a copy to avoid mutating the hook's internal state
				sorted := make([]string, len(aggregates))
				copy(sorted, aggregates)
				// Sort aggregates for consistent comparison
				sort.Strings(sorted)
				// Join aggregates into a single string
				aggregatesStr := strings.Join(sorted, ", ")
				// Assert equality
				assert.Equal(t, tt.ExpectedAggregates, aggregatesStr)
			}
		})
	}
}
