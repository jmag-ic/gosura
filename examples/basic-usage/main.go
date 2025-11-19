package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmag-ic/gosura/pkg/hooks/postgres"
	"github.com/jmag-ic/gosura/pkg/hooks/sql"
	"github.com/jmag-ic/gosura/pkg/inspector"
)

func main() {
	fmt.Println("🚀 Gosura Filter Inspector - Basic Usage Example")
	fmt.Println("================================================")
	fmt.Println()

	// Create reusable instances
	inspectorInstance := &inspector.HasuraInspector{}
	config := postgres.NewParseHookConfig()
	ctx := context.Background()

	// Example 1: Basic WHERE clause
	fmt.Println("Example 1: Basic WHERE Clause")
	fmt.Println("------------------------------")
	filterJSON := `{
		"where": {
			"_and": [
				{"age": {"_gte": 25}},
				{"name": {"_like": "%John%"}}
			]
		}
	}`

	pgParserHook := sql.NewSQLParseHook(config)
	err := inspectorInstance.Inspect(ctx, filterJSON, pgParserHook)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	whereClause, params := pgParserHook.GetWhereClause()
	fmt.Printf("✅ Generated SQL: SELECT * FROM users WHERE %s\n", whereClause)
	fmt.Printf("📝 Parameters: %v\n\n", params)

	// Example 2: Aggregations
	fmt.Println("Example 2: Aggregations")
	fmt.Println("------------------------")
	filterJSON = `{
		"where": {
			"status": {"_eq": "active"}
		},
		"aggregate": {
			"count": "*",
			"sum": ["price", "quantity"],
			"avg": "rating",
			"max": "created_at"
		},
		"order_by": {
			"avg_rating": "desc"
		}
	}`

	pgParserHook = sql.NewSQLParseHook(config)
	err = inspectorInstance.Inspect(ctx, filterJSON, pgParserHook)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	aggregates := pgParserHook.GetAggregates()
	whereClause, params = pgParserHook.GetWhereClause()
	orderByClause := pgParserHook.GetOrderByClause()

	// User composes the SELECT clause
	selectClause := strings.Join(aggregates, ", ")

	fmt.Printf("✅ Generated SQL:\n")
	fmt.Printf("   SELECT %s\n", selectClause)
	fmt.Printf("   FROM products\n")
	if whereClause != "" {
		fmt.Printf("   WHERE %s\n", whereClause)
	}
	if orderByClause != "" {
		fmt.Printf("   ORDER BY %s\n", orderByClause)
	}
	fmt.Printf("📝 Parameters: %v\n\n", params)

	// Example 3: Count with DISTINCT
	fmt.Println("Example 3: Count with DISTINCT")
	fmt.Println("-------------------------------")
	filterJSON = `{
		"aggregate": {
			"count": {
				"field": "user_id",
				"distinct": true
			}
		}
	}`

	pgParserHook = sql.NewSQLParseHook(config)
	err = inspectorInstance.Inspect(ctx, filterJSON, pgParserHook)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	aggregates = pgParserHook.GetAggregates()
	selectClause = strings.Join(aggregates, ", ")

	fmt.Printf("✅ Generated SQL: SELECT %s FROM orders\n", selectClause)
	fmt.Println()

	// Example 4: Pagination with LIMIT and OFFSET
	fmt.Println("Example 4: Pagination (LIMIT & OFFSET)")
	fmt.Println("---------------------------------------")
	filterJSON = `{
		"where": {
			"status": {"_eq": "active"}
		},
		"order_by": {
			"created_at": "desc"
		},
		"limit": 10,
		"offset": 20
	}`

	pgParserHook = sql.NewSQLParseHook(config)
	err = inspectorInstance.Inspect(ctx, filterJSON, pgParserHook)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	whereClause, params = pgParserHook.GetWhereClause()
	orderByClause = pgParserHook.GetOrderByClause()
	limit := pgParserHook.GetLimit()
	offset := pgParserHook.GetOffset()

	// Build the complete SQL query
	query := "SELECT * FROM products"
	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}
	if orderByClause != "" {
		query += fmt.Sprintf(" ORDER BY %s", orderByClause)
	}
	if limit != nil {
		query += fmt.Sprintf(" LIMIT %d", *limit)
	}
	if offset != nil {
		query += fmt.Sprintf(" OFFSET %d", *offset)
	}

	fmt.Printf("✅ Generated SQL:\n")
	fmt.Printf("   %s\n", query)
	fmt.Printf("📝 Parameters: %v\n", params)
	fmt.Printf("📄 Returns: Rows 21-30 of active products, newest first\n")

	fmt.Println("\n🎉 All examples completed successfully!")
}
