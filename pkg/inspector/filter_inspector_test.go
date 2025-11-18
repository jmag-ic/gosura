package inspector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
)

func runTestCases(t *testing.T, tests []filterInspectorTestCase) {
	ctx := context.Background()
	hasuraInspector := &HasuraInspector{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hook := &TestHook{t: t}
			err := hasuraInspector.Inspect(ctx, tt.filter, hook)
			tt.validate(hook, err)
		})
	}
}

type filterInspectorTestCase struct {
	name     string
	filter   string
	validate func(*TestHook, error)
}

type comparisonCall struct {
	field, operator string
	path            []string
}

type orderByCall struct {
	field, direction string
	path             []string
}

type nestedNodeCall struct {
	field  string
	source string
}

type aggregateFieldCall struct {
	function, field string
}

// TestHook implements FilterHook for testing
type TestHook struct {
	t *testing.T
	// Track calls for verification
	comparisonCalls      []comparisonCall
	nestedNodeStartCalls []nestedNodeCall
	nestedNodeEndCalls   []nestedNodeCall
	logicalOpStartCalls  []string
	logicalOpEndCalls    []string
	orderByCalls         []orderByCall
	aggregateFieldCalls  []aggregateFieldCall
}

func (h *TestHook) OnComparison(ctx context.Context, field string, operator string, value gjson.Result, path []string) error {
	h.comparisonCalls = append(h.comparisonCalls, comparisonCall{field, operator, path})
	return nil
}

func (h *TestHook) OnNestedNodeStart(ctx context.Context, field string, node gjson.Result, src string, path []string) {
	h.nestedNodeStartCalls = append(h.nestedNodeStartCalls, nestedNodeCall{field, src})
}

func (h *TestHook) OnNestedNodeEnd(ctx context.Context, field string, node gjson.Result, src string, path []string) {
	h.nestedNodeEndCalls = append(h.nestedNodeEndCalls, nestedNodeCall{field, src})
}

func (h *TestHook) OnLogicalGroupStart(ctx context.Context, operator string, node gjson.Result, path []string) error {
	h.logicalOpStartCalls = append(h.logicalOpStartCalls, operator)
	return nil
}

func (h *TestHook) OnLogicalGroupEnd(ctx context.Context, operator string, node gjson.Result, path []string) {
	h.logicalOpEndCalls = append(h.logicalOpEndCalls, operator)
}

func (h *TestHook) OnOrderBy(ctx context.Context, field string, direction string, path []string) {
	h.orderByCalls = append(h.orderByCalls, orderByCall{field, direction, path})
}

func (h *TestHook) OnAggregateField(ctx context.Context, function string, field string, options gjson.Result) error {
	h.aggregateFieldCalls = append(h.aggregateFieldCalls, aggregateFieldCall{function, field})
	return nil
}

func TestFilterInspector(t *testing.T) {
	tests := []filterInspectorTestCase{
		{
			name:   "Empty filter string",
			filter: "",
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 0, len(h.comparisonCalls))
				assert.Equal(t, 0, len(h.nestedNodeStartCalls))
				assert.Equal(t, 0, len(h.nestedNodeEndCalls))
				assert.Equal(t, 0, len(h.logicalOpStartCalls))
				assert.Equal(t, 0, len(h.logicalOpEndCalls))
			},
		},
		{
			name:   "Empty filter",
			filter: `{}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 0, len(h.comparisonCalls))
				assert.Equal(t, 0, len(h.nestedNodeStartCalls))
				assert.Equal(t, 0, len(h.nestedNodeEndCalls))
				assert.Equal(t, 0, len(h.logicalOpStartCalls))
				assert.Equal(t, 0, len(h.logicalOpEndCalls))
			},
		},
		{
			name:   "Empty where",
			filter: `{"where":{}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 0, len(h.comparisonCalls))
				assert.Equal(t, 0, len(h.nestedNodeStartCalls))
			},
		},
		{
			name:   "Empty order_by",
			filter: `{"order_by":{}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 0, len(h.orderByCalls))
			},
		},
		{
			name:   "Simple comparison",
			filter: `{"where":{"age":{"_gt": 18}}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				if len(h.comparisonCalls) != 1 {
					t.Errorf("Expected 1 filter call, got %d", len(h.comparisonCalls))
				}
				if h.comparisonCalls[0].field != "age" || h.comparisonCalls[0].operator != "_gt" {
					t.Errorf("Unexpected filter call: %+v", h.comparisonCalls[0])
				}
				if len(h.comparisonCalls[0].path) != 0 {
					t.Errorf("Unexpected filter call path: %+v", h.comparisonCalls[0].path)
				}
			},
		},
		{
			name:   "Logical operators",
			filter: `{"where":{"_and": [{"age":{"_gt":18}},{"name":{"_like":"%John%"}}]}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				if len(h.logicalOpStartCalls) != 1 {
					t.Errorf("Expected 1 logical operator call, got %d", len(h.logicalOpStartCalls))
				}
				if h.logicalOpStartCalls[0] != "_and" {
					t.Errorf("Unexpected logical operator call: %+v", h.logicalOpStartCalls[0])
				}
				if len(h.comparisonCalls) != 2 {
					t.Errorf("Expected 2 filter calls, got %d", len(h.comparisonCalls))
				}
				if len(h.logicalOpEndCalls) != 1 {
					t.Errorf("Expected 1 logical operator end call, got %d", len(h.logicalOpEndCalls))
				}
				if h.logicalOpEndCalls[0] != "_and" {
					t.Errorf("Unexpected logical operator end call: %+v", h.logicalOpEndCalls[0])
				}
			},
		},
		{
			name:   "Nested where filters",
			filter: `{"where":{"address":{"city":{"_eq":"New York"}}}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				if len(h.nestedNodeStartCalls) != 2 {
					t.Errorf("Expected 2 nested filter calls, got %d", len(h.nestedNodeStartCalls))
				}
				expectedWhereCalls := sliceFilter(h.nestedNodeStartCalls, func(el nestedNodeCall) bool {
					return el.source == "where"
				})
				if len(expectedWhereCalls) != 2 {
					t.Errorf("Expected 2 nested filter calls, got %d", len(expectedWhereCalls))
				}
				if h.nestedNodeStartCalls[0].field != "address" {
					t.Errorf("Unexpected nested filter call: %s", h.nestedNodeStartCalls[0])
				}
				if h.nestedNodeStartCalls[1].field != "city" {
					t.Errorf("Unexpected nested filter call: %s", h.nestedNodeStartCalls[1])
				}
				if len(h.comparisonCalls) != 1 {
					t.Errorf("Expected 1 filter call, got %d", len(h.comparisonCalls))
				}
				if h.comparisonCalls[0].operator != "_eq" {
					t.Errorf("Unexpected filter call operator: %s", h.comparisonCalls[0].operator)
				}
				if h.comparisonCalls[0].field != "city" {
					t.Errorf("Unexpected filter call field: %s", h.comparisonCalls[0].field)
				}
				if len(h.comparisonCalls[0].path) != 1 {
					t.Errorf("Expected 1 element in path, got %d", len(h.comparisonCalls[0].path))
				}
				if h.comparisonCalls[0].path[0] != "address" {
					t.Errorf("Unexpected filter call path: %+v", h.comparisonCalls[0].path)
				}
			},
		},
		{
			name:   "Three levels of nested where filters",
			filter: `{"where":{"user":{"profile":{"address":{"city":{"_eq":"New York"}}}}}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				if len(h.nestedNodeStartCalls) != 4 {
					t.Errorf("Expected 4 nested filter calls, got %d", len(h.nestedNodeStartCalls))
				}
				expectedWhereCalls := sliceFilter(h.nestedNodeStartCalls, func(el nestedNodeCall) bool {
					return el.source == "where"
				})
				if len(expectedWhereCalls) != 4 {
					t.Errorf("Expected 4 nested where filter calls, got %d", len(expectedWhereCalls))
				}
				expectedNestedCalls := []nestedNodeCall{
					{field: "user", source: "where"},
					{field: "profile", source: "where"},
					{field: "address", source: "where"},
					{field: "city", source: "where"},
				}
				for i, expected := range expectedNestedCalls {
					if i >= len(h.nestedNodeStartCalls) {
						t.Errorf("Missing nested filter call for '%s'", expected)
						continue
					}
					if h.nestedNodeStartCalls[i] != expected {
						t.Errorf("Expected nested filter call %d to be '%s', got '%s'", i, expected, h.nestedNodeStartCalls[i])
					}
				}
				if len(h.comparisonCalls) != 1 {
					t.Errorf("Expected 1 filter call, got %d", len(h.comparisonCalls))
				}
				if h.comparisonCalls[0].field != "city" || h.comparisonCalls[0].operator != "_eq" {
					t.Errorf("Unexpected filter call: %+v", h.comparisonCalls[0])
				}
			},
		},
		{
			name:   "Empty logical operators",
			filter: `{"where":{"_and":[{"_and":[]},{"_or":[]}]}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 3, len(h.logicalOpStartCalls))
				assert.Equal(t, 3, len(h.logicalOpEndCalls))
				assert.Equal(t, "_and", h.logicalOpStartCalls[0])
				assert.Equal(t, "_and", h.logicalOpEndCalls[0])
				assert.Equal(t, "_and", h.logicalOpStartCalls[1])
				assert.Equal(t, "_or", h.logicalOpEndCalls[1])
				assert.Equal(t, "_or", h.logicalOpStartCalls[2])
				assert.Equal(t, "_and", h.logicalOpEndCalls[2])
			},
		},
		{
			name:   "Syntactic sugar",
			filter: `{"where":{"age":18,"name":"John"}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 0, len(h.logicalOpStartCalls))
				assert.Equal(t, 2, len(h.comparisonCalls))
				assert.Equal(t, "age", h.comparisonCalls[0].field)
				assert.Equal(t, "_eq", h.comparisonCalls[0].operator)
				assert.Equal(t, "name", h.comparisonCalls[1].field)
				assert.Equal(t, "_eq", h.comparisonCalls[1].operator)
			},
		},
		{
			name:   "_and as object",
			filter: `{"where":{"_and":{"age":{"_gt":18}}}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 1, len(h.logicalOpStartCalls))
				assert.Equal(t, 1, len(h.logicalOpEndCalls))
				assert.Equal(t, "_and", h.logicalOpStartCalls[0])
				assert.Equal(t, "_and", h.logicalOpEndCalls[0])
				assert.Equal(t, 1, len(h.comparisonCalls))
				assert.Equal(t, "age", h.comparisonCalls[0].field)
				assert.Equal(t, "_gt", h.comparisonCalls[0].operator)
			},
		},
		{
			name:   "_or as object",
			filter: `{"where":{"_or":{"age":{"_gt":18}}}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 1, len(h.logicalOpStartCalls))
				assert.Equal(t, 1, len(h.logicalOpEndCalls))
				assert.Equal(t, "_or", h.logicalOpStartCalls[0])
				assert.Equal(t, "_or", h.logicalOpEndCalls[0])
				assert.Equal(t, 1, len(h.comparisonCalls))
				assert.Equal(t, "age", h.comparisonCalls[0].field)
				assert.Equal(t, "_gt", h.comparisonCalls[0].operator)
			},
		},
		{
			name:   "_not as array",
			filter: `{"where":{"_not":[{"age":{"_gt":18}}]}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 1, len(h.logicalOpStartCalls))
				assert.Equal(t, 1, len(h.logicalOpEndCalls))
				assert.Equal(t, "_not", h.logicalOpStartCalls[0])
				assert.Equal(t, "_not", h.logicalOpEndCalls[0])
				assert.Equal(t, 1, len(h.comparisonCalls))
				assert.Equal(t, "age", h.comparisonCalls[0].field)
				assert.Equal(t, "_gt", h.comparisonCalls[0].operator)
			},
		},
		{
			name: "Nested logical operators as objects",
			filter: `{
				"where":{
					"name":"jose",
					"_and":{"age":18,"role":"admin"},
					"_or":{"age":{"_lt":18}, "role":"user"},
					"_not":{"deleted":true}
				}
			}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 6, len(h.comparisonCalls))
				assert.Equal(t, "name", h.comparisonCalls[0].field)
				assert.Equal(t, "_eq", h.comparisonCalls[0].operator)
				assert.Equal(t, "age", h.comparisonCalls[1].field)
				assert.Equal(t, "_eq", h.comparisonCalls[1].operator)
				assert.Equal(t, "role", h.comparisonCalls[2].field)
				assert.Equal(t, "_eq", h.comparisonCalls[2].operator)
				assert.Equal(t, "age", h.comparisonCalls[3].field)
				assert.Equal(t, "_lt", h.comparisonCalls[3].operator)
				assert.Equal(t, "role", h.comparisonCalls[4].field)
				assert.Equal(t, "_eq", h.comparisonCalls[4].operator)
				assert.Equal(t, "deleted", h.comparisonCalls[5].field)
				assert.Equal(t, "_eq", h.comparisonCalls[5].operator)
				assert.Equal(t, 3, len(h.logicalOpStartCalls))
				assert.Equal(t, 3, len(h.logicalOpEndCalls))
				assert.Equal(t, "_and", h.logicalOpStartCalls[0])
				assert.Equal(t, "_and", h.logicalOpEndCalls[0])
				assert.Equal(t, "_or", h.logicalOpStartCalls[1])
				assert.Equal(t, "_or", h.logicalOpEndCalls[1])
				assert.Equal(t, "_not", h.logicalOpStartCalls[2])
			},
		},
		{
			name:   "Order by array",
			filter: `{"order_by":[{"name":"asc"},{"age":"desc"}]}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 2, len(h.orderByCalls))
				assert.Equal(t, "name", h.orderByCalls[0].field)
				assert.Equal(t, "ASC", h.orderByCalls[0].direction)
				assert.Equal(t, "age", h.orderByCalls[1].field)
				assert.Equal(t, "DESC", h.orderByCalls[1].direction)
			},
		},
		{
			name:   "Order by object",
			filter: `{"order_by":{"name":"asc","age": "desc"}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 2, len(h.orderByCalls))
				assert.Equal(t, "name", h.orderByCalls[0].field)
				assert.Equal(t, "ASC", h.orderByCalls[0].direction)
				assert.Equal(t, "age", h.orderByCalls[1].field)
				assert.Equal(t, "DESC", h.orderByCalls[1].direction)
			},
		},
		{
			name:   "Order by nested",
			filter: `{"order_by":{"user":{"name":"asc"}}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 1, len(h.orderByCalls))
				assert.Equal(t, "name", h.orderByCalls[0].field)
				assert.Equal(t, "ASC", h.orderByCalls[0].direction)
				// check the nested calls
				assert.Equal(t, 1, len(h.nestedNodeStartCalls))
				assert.Equal(t, "user", h.nestedNodeStartCalls[0].field)
				assert.Equal(t, "order_by", h.nestedNodeStartCalls[0].source)
			},
		},

		{
			name:   "Null value",
			filter: `{"where":{"age":null}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 1, len(h.comparisonCalls))
				assert.Equal(t, "age", h.comparisonCalls[0].field)
				assert.Equal(t, "_is_null", h.comparisonCalls[0].operator)
			},
		},
	}

	runTestCases(t, tests)
}

func TestFilterInspectorErrors(t *testing.T) {
	tests := []filterInspectorTestCase{
		{
			name:   "Empty key in filter",
			filter: `{"where":{"":{_eq:18}}}`,
			validate: func(h *TestHook, err error) {
				// validate the error
				assert.Error(t, err)
				assert.Equal(t, "empty key found in path: where", err.Error())

				// check the hook calls
				assert.Equal(t, 0, len(h.comparisonCalls))
				assert.Equal(t, 0, len(h.nestedNodeStartCalls))
				assert.Equal(t, 0, len(h.nestedNodeEndCalls))
				assert.Equal(t, 0, len(h.logicalOpStartCalls))
				assert.Equal(t, 0, len(h.logicalOpEndCalls))
			},
		},
		{
			name:   "Empty key in a where nested node",
			filter: `{"where":{"user":{"":{_eq:18}}}}`,
			validate: func(h *TestHook, err error) {
				// validate the error
				assert.Error(t, err)
				assert.Equal(t, "empty key found in path: where.user", err.Error())
			},
		},
		{
			name:   "Empty key in a order_by nested node",
			filter: `{"order_by":{"user":{"":"desc"}}}`,
			validate: func(h *TestHook, err error) {
				assert.Error(t, err)
				assert.Equal(t, "empty key found in path: order_by.user", err.Error())
			},
		},
		{
			name:   "Invalid order_by direction",
			filter: `{"order_by":[{"name":"invalid"}]}`,
			validate: func(h *TestHook, err error) {
				assert.Error(t, err)
				assert.Equal(t, "invalid order_by direction: invalid", err.Error())
			},
		},
		{
			name:   "Invalid filter node",
			filter: `{"where":18}`,
			validate: func(h *TestHook, err error) {
				assert.Error(t, err)
				assert.Equal(t, "invalid filter node: where", err.Error())
			},
		},
	}

	runTestCases(t, tests)
}

func TestAggregateInspector(t *testing.T) {
	tests := []filterInspectorTestCase{
		{
			name:   "Empty aggregate",
			filter: `{"aggregate":{}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 0, len(h.aggregateFieldCalls))
			},
		},
		{
			name:   "Simple count aggregate",
			filter: `{"aggregate":{"count":"*"}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 1, len(h.aggregateFieldCalls))
				assert.Equal(t, "count", h.aggregateFieldCalls[0].function)
				assert.Equal(t, "*", h.aggregateFieldCalls[0].field)
			},
		},
		{
			name:   "Single field aggregate",
			filter: `{"aggregate":{"sum":"price"}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 1, len(h.aggregateFieldCalls))
				assert.Equal(t, "sum", h.aggregateFieldCalls[0].function)
				assert.Equal(t, "price", h.aggregateFieldCalls[0].field)
			},
		},
		{
			name:   "Multiple fields in single aggregate",
			filter: `{"aggregate":{"sum":["price","quantity"]}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 2, len(h.aggregateFieldCalls))
				assert.Equal(t, "price", h.aggregateFieldCalls[0].field)
				assert.Equal(t, "quantity", h.aggregateFieldCalls[1].field)
			},
		},
		{
			name:   "Multiple aggregate functions",
			filter: `{"aggregate":{"count":"*","sum":"price","avg":"rating"}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 3, len(h.aggregateFieldCalls))
				// Note: order may vary in JSON parsing, so we check all are present
				functions := make(map[string]bool)
				for _, call := range h.aggregateFieldCalls {
					functions[call.function] = true
				}
				assert.True(t, functions["count"])
				assert.True(t, functions["sum"])
				assert.True(t, functions["avg"])
			},
		},
		{
			name:   "Count with distinct option",
			filter: `{"aggregate":{"count":{"field":"user_id","distinct":true}}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 1, len(h.aggregateFieldCalls))
				assert.Equal(t, "count", h.aggregateFieldCalls[0].function)
				assert.Equal(t, "user_id", h.aggregateFieldCalls[0].field)
			},
		},
		{
			name:   "Aggregate with where clause",
			filter: `{"where":{"status":{"_eq":"active"}},"aggregate":{"count":"*","avg":"rating"}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 1, len(h.comparisonCalls))
				assert.Equal(t, "status", h.comparisonCalls[0].field)
				assert.Equal(t, 2, len(h.aggregateFieldCalls))
			},
		},
		{
			name:   "Aggregate with order by",
			filter: `{"aggregate":{"count":"*"},"order_by":{"count":"desc"}}`,
			validate: func(h *TestHook, err error) {
				assert.Nil(t, err)
				assert.Equal(t, 1, len(h.orderByCalls))
				assert.Equal(t, "count", h.orderByCalls[0].field)
				assert.Equal(t, "DESC", h.orderByCalls[0].direction)
			},
		},
		{
			name:   "Invalid aggregate node",
			filter: `{"aggregate":18}`,
			validate: func(h *TestHook, err error) {
				assert.Error(t, err)
				assert.Equal(t, "invalid aggregate node: must be an object", err.Error())
			},
		},
		{
			name:   "Empty aggregate function name",
			filter: `{"aggregate":{"":["price"]}}`,
			validate: func(h *TestHook, err error) {
				assert.Error(t, err)
				assert.Equal(t, "empty aggregate function name", err.Error())
			},
		},
	}

	runTestCases(t, tests)
}

func sliceFilter[T any](slice []T, fn func(T) bool) []T {
	filtered := make([]T, 0)
	for _, el := range slice {
		if !fn(el) {
			continue
		}
		filtered = append(filtered, el)
	}

	return filtered
}
