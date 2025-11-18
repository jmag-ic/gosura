package inspector

import (
	"context"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

const (
	// Logical operators
	OpAnd = "_and"
	OpOr  = "_or"
	OpNot = "_not"
	// Order by directions
	DirAsc  = "ASC"
	DirDesc = "DESC"
	// Hasura filter elements
	KeyWhere     = "where"
	KeyOrderBy   = "order_by"
	KeyAggregate = "aggregate"
)

var logicalOperators = map[string]struct{}{OpAnd: {}, OpOr: {}, OpNot: {}}

// FilterHook defines the interface for hooks that can process filter operations
type FilterHook interface {
	// OnComparison is called when a field with an operator is encountered
	OnComparison(ctx context.Context, field string, operator string, value gjson.Result, path []string) error
	// OnNestedNodeStart is called when a nested node is encountered
	OnNestedNodeStart(ctx context.Context, field string, node gjson.Result, src string, path []string)
	// OnNestedNodeEnd is called when a nested node is complete
	OnNestedNodeEnd(ctx context.Context, field string, node gjson.Result, src string, path []string)
	// OnLogicalGroupStart is called when a logical operator (_and, _or, _not) is encountered
	OnLogicalGroupStart(ctx context.Context, operator string, node gjson.Result, path []string) error
	// OnLogicalGroupEnd is called when a logical group is complete
	OnLogicalGroupEnd(ctx context.Context, operator string, node gjson.Result, path []string)
	// OnOrderBy is called when a field is ordered
	OnOrderBy(ctx context.Context, field string, direction string, path []string)
	// OnAggregateField is called for each field in an aggregate function
	OnAggregateField(ctx context.Context, function string, field string, options gjson.Result) error
}

// HasuraInspector
type HasuraInspector struct{}

// Inspect processes a filter JSON string
func (hi *HasuraInspector) Inspect(ctx context.Context, filterJSON string, hooks ...FilterHook) error {
	filter := gjson.Parse(filterJSON)

	if where := filter.Get(KeyWhere); where.Exists() {
		if err := hi.processWhereNode(ctx, hooks, "", where, []string{}); err != nil {
			return err
		}
	}

	if aggregate := filter.Get(KeyAggregate); aggregate.Exists() {
		if err := hi.processAggregateNode(ctx, hooks, aggregate); err != nil {
			return err
		}
	}

	if orderBy := filter.Get(KeyOrderBy); orderBy.Exists() {
		if err := hi.processOrderByNode(ctx, hooks, "", orderBy, []string{}); err != nil {
			return err
		}
	}

	return nil
}

// Processors

// processWhereNode recursively processes a JSON node
func (hi *HasuraInspector) processWhereNode(ctx context.Context, hooks []FilterHook, field string, node gjson.Result, path []string) error {
	if node.IsArray() {
		for _, child := range node.Array() {
			if err := hi.processWhereNode(ctx, hooks, field, child, path); err != nil {
				return err
			}
		}
		return nil
	}

	if !node.IsObject() {
		return fmt.Errorf("invalid filter node: %s", hi.getStrPath(KeyWhere, path))
	}

	var err error
	node.ForEach(func(k, value gjson.Result) bool {
		key := k.String()

		// Validate the key
		if strings.TrimSpace(key) == "" {
			err = fmt.Errorf("empty key found in path: %s", hi.getStrPath(KeyWhere, path))
			return false
		}

		// Key is an operator
		if key[0] == '_' {

			previousPath := path
			if len(path) > 0 {
				previousPath = hi.buildPath(path[:len(path)-1], "")
			}

			// Key is a logical operator
			if _, ok := logicalOperators[key]; ok {
				if err = hi.notifyLogicalGroupStart(ctx, hooks, key, node, path); err != nil {
					return false
				}

				if err = hi.processWhereNode(ctx, hooks, field, value, previousPath); err != nil {
					return false
				}

				hi.notifyLogicalGroupEnd(ctx, hooks, key, node, path)
			} else {
				// Key is a comparison operator
				if err = hi.notifyComparison(ctx, hooks, field, key, value, previousPath); err != nil {
					return false
				}
			}

		} else if value.IsObject() {
			// Key is the name of a nested node
			hi.notifyNestedNodeStart(ctx, hooks, key, value, KeyWhere, path)

			nestedPath := hi.buildPath(path, key)
			if err = hi.processWhereNode(ctx, hooks, key, value, nestedPath); err != nil {
				return false
			}

			hi.notifyNestedNodeEnd(ctx, hooks, key, value, KeyWhere, path)

		} else {
			if value.Type == gjson.Null {
				err = hi.notifyComparison(ctx, hooks, key, "_is_null", gjson.Result{Type: gjson.String, Str: "true"}, path)
			} else {
				err = hi.notifyComparison(ctx, hooks, key, "_eq", value, path)
			}
			if err != nil {
				return false
			}
		}

		return true
	})

	return err
}

func (hi *HasuraInspector) processOrderByNode(ctx context.Context, hooks []FilterHook, field string, node gjson.Result, path []string) error {
	if node.IsArray() {
		for _, child := range node.Array() {
			if err := hi.processOrderByNode(ctx, hooks, field, child, path); err != nil {
				return err
			}
		}
		return nil
	}

	if !node.IsObject() {
		return fmt.Errorf("invalid order_by node: %s", hi.getStrPath(KeyOrderBy, path))
	}

	var err error
	node.ForEach(func(k, value gjson.Result) bool {
		key := k.String()

		if strings.TrimSpace(key) == "" {
			err = fmt.Errorf("empty key found in path: %s", hi.getStrPath(KeyOrderBy, path))
			return false
		}

		if value.IsObject() {
			// A nested node was found
			hi.notifyNestedNodeStart(ctx, hooks, key, value, KeyOrderBy, path)

			nestedPath := hi.buildPath(path, key)
			if err = hi.processOrderByNode(ctx, hooks, key, value, nestedPath); err != nil {
				return false
			}

			hi.notifyNestedNodeEnd(ctx, hooks, key, value, KeyOrderBy, path)

		} else {
			var direction string
			direction, err = hi.getOrderDirection(value.Str)
			if err != nil {
				return false
			}

			hi.notifyOnOrderBy(ctx, hooks, key, path, direction)

		}

		return true
	})

	return err
}

// Notifiers

// notifyLogicalGroupStart is a helper function to notify hooks about a logical group start
func (hi *HasuraInspector) notifyLogicalGroupStart(ctx context.Context, hooks []FilterHook, operator string, node gjson.Result, path []string) error {
	for _, hook := range hooks {
		if err := hook.OnLogicalGroupStart(ctx, operator, node, path); err != nil {
			return err
		}
	}
	return nil
}

// notifyLogicalGroupEnd is a helper function to notify hooks about a logical group end
func (hi *HasuraInspector) notifyLogicalGroupEnd(ctx context.Context, hooks []FilterHook, operator string, node gjson.Result, path []string) {
	for _, hook := range hooks {
		hook.OnLogicalGroupEnd(ctx, operator, node, path)
	}
}

// notifyNestedNodeStart is a helper function to notify hooks about a nested node start
func (hi *HasuraInspector) notifyNestedNodeStart(ctx context.Context, hooks []FilterHook, field string, node gjson.Result, src string, path []string) {
	for _, hook := range hooks {
		hook.OnNestedNodeStart(ctx, field, node, src, path)
	}
}

// notifyNestedNodeEnd is a helper function to notify hooks about a nested node end
func (hi *HasuraInspector) notifyNestedNodeEnd(ctx context.Context, hooks []FilterHook, field string, node gjson.Result, src string, path []string) {
	for _, hook := range hooks {
		hook.OnNestedNodeEnd(ctx, field, node, src, path)
	}
}

// notifyComparison is a helper function to notify hooks about a comparison
func (hi *HasuraInspector) notifyComparison(ctx context.Context, hooks []FilterHook, field string, operator string, value gjson.Result, path []string) error {
	for _, hook := range hooks {
		if err := hook.OnComparison(ctx, field, operator, value, path); err != nil {
			return err
		}
	}
	return nil
}

// notifyOnOrderBy is a helper function to notify hooks about an field order
func (hi *HasuraInspector) notifyOnOrderBy(ctx context.Context, hooks []FilterHook, field string, path []string, direction string) {
	for _, hook := range hooks {
		hook.OnOrderBy(ctx, field, direction, path)
	}
}

// Utils

// buildPath builds a path from a list of elements
func (hi *HasuraInspector) buildPath(path []string, elements ...string) []string {
	newPath := make([]string, len(path), len(path)+len(elements))
	copy(newPath, path)

	for _, e := range elements {
		if trimmed := strings.TrimSpace(e); trimmed != "" {
			newPath = append(newPath, trimmed)
		}
	}
	return newPath
}

// getStrPath builds a path string from a list of elements
func (hi *HasuraInspector) getStrPath(src string, path []string) string {
	allPathElements := make([]string, 0, len(path)+1)
	allPathElements = append(allPathElements, src)
	allPathElements = append(allPathElements, path...)

	return strings.Join(allPathElements, ".")
}

// getOrderDirection get the order direction based on a string
func (hi *HasuraInspector) getOrderDirection(str string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(str)) {
	case DirAsc:
		return DirAsc, nil
	case DirDesc:
		return DirDesc, nil
	default:
		return "", fmt.Errorf("invalid order_by direction: %s", str)
	}
}

// processAggregateNode processes the aggregate JSON node
func (hi *HasuraInspector) processAggregateNode(ctx context.Context, hooks []FilterHook, node gjson.Result) error {
	if !node.IsObject() {
		return fmt.Errorf("invalid aggregate node: must be an object")
	}

	var err error
	node.ForEach(func(k, value gjson.Result) bool {
		aggregateFn := k.String()

		if strings.TrimSpace(aggregateFn) == "" {
			err = fmt.Errorf("empty aggregate function name")
			return false
		}

		// Parse fields based on value type
		fields, options := hi.parseAggregateValue(value)
		if fields == nil {
			err = fmt.Errorf("invalid value for aggregate function %s", aggregateFn)
			return false
		}

		// Process each field
		for _, field := range fields {
			if err = hi.notifyAggregateField(ctx, hooks, aggregateFn, field, options); err != nil {
				return false
			}
		}

		return true
	})

	return err
}

// parseAggregateValue extracts fields from different value formats
func (hi *HasuraInspector) parseAggregateValue(value gjson.Result) ([]string, gjson.Result) {
	switch value.Type {
	case gjson.String:
		// Single field: "sum": "price"
		return []string{value.String()}, gjson.Result{}

	case gjson.JSON:
		if value.IsArray() {
			// Multiple fields: "sum": ["price", "quantity"]
			arr := value.Array()
			if len(arr) == 0 {
				return nil, gjson.Result{}
			}

			fields := make([]string, 0, len(arr))
			for _, v := range arr {
				if v.Type != gjson.String {
					return nil, gjson.Result{}
				}
				fields = append(fields, v.String())
			}
			return fields, gjson.Result{}

		} else if value.IsObject() {
			// Advanced options: "count": { "field": "id", "distinct": true }
			field := value.Get("field").String()
			if field == "" {
				field = "*"
			}
			return []string{field}, value
		}
	}

	return nil, gjson.Result{}
}

// notifyAggregateField notifies hooks about an aggregate field
func (hi *HasuraInspector) notifyAggregateField(ctx context.Context, hooks []FilterHook, function, field string, options gjson.Result) error {
	for _, hook := range hooks {
		if err := hook.OnAggregateField(ctx, function, field, options); err != nil {
			return err
		}
	}
	return nil
}
