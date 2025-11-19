package main

import (
	"context"
	"fmt"

	"github.com/jmag-ic/gosura/hooks/postgres"
	"github.com/jmag-ic/gosura/hooks/sql"
	"github.com/jmag-ic/gosura/inspector"
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

	filter := sql.NewSQLFilter(config)
	err := inspectorInstance.Inspect(ctx, filterJSON, filter)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	queryBuilder := filter.GetQueryBuilder()
	query := queryBuilder.Build("users")

	fmt.Printf("✅ Generated SQL: %s\n", query)
	fmt.Printf("📝 Parameters: %v\n\n", queryBuilder.Params)

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

	filter = sql.NewSQLFilter(config)
	err = inspectorInstance.Inspect(ctx, filterJSON, filter)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	queryBuilder = filter.GetQueryBuilder()
	query = queryBuilder.Build("products")

	fmt.Printf("✅ Generated SQL: %s\n", query)
	fmt.Printf("📝 Parameters: %v\n\n", queryBuilder.Params)

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

	filter = sql.NewSQLFilter(config)
	err = inspectorInstance.Inspect(ctx, filterJSON, filter)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	queryBuilder = filter.GetQueryBuilder()
	query = queryBuilder.Build("orders")

	fmt.Printf("✅ Generated SQL: %s\n", query)
	fmt.Printf("📝 Parameters: %v\n\n", queryBuilder.Params)

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

	filter = sql.NewSQLFilter(config)
	err = inspectorInstance.Inspect(ctx, filterJSON, filter)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	queryBuilder = filter.GetQueryBuilder()
	query = queryBuilder.Build("orders")

	fmt.Printf("✅ Generated SQL: %s\n", query)
	fmt.Printf("📝 Parameters: %v\n\n", queryBuilder.Params)

	fmt.Println("\n🎉 All examples completed successfully!")
}
