package sql

import (
	"context"
	"testing"

	"github.com/jmag-ic/gosura/pkg/inspector"
	"github.com/stretchr/testify/assert"
)

type SQLParseTestCase struct {
	Name            string
	Filter          string
	ExpectedWhere   string
	ExpectedOrderBy string
	Params          []any
	ValidateErr     func(error)
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
		})
	}
}
