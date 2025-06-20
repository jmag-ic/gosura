package main

import (
	"context"
	"fmt"

	"github.com/jmag-ic/gosura/pkg/hooks/postgres"
	"github.com/jmag-ic/gosura/pkg/hooks/sql"
	"github.com/jmag-ic/gosura/pkg/inspector"
)

func main() {
	fmt.Println("🚀 Gosura Filter Inspector - Basic Usage Example")
	fmt.Println("================================================")

	// Create a Hasura filter
	filterJSON := `{
		"where": {
			"_and": [
				{"age": {"_gte": 25}},
				{"name": {"_like": "%John%"}}
			]
		}
	}`

	fmt.Printf("Filter: %s\n\n", filterJSON)

	// Create inspector and hook
	inspector := &inspector.HasuraInspector{}
	sqlParseHook := sql.NewSQLParseHook(postgres.NewParseHookConfig())

	// Process the filter
	err := inspector.Inspect(context.Background(), filterJSON, sqlParseHook)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		return
	}

	// Get the generated SQL
	whereClause, params := sqlParseHook.GetWhereClause()

	fmt.Printf("✅ Generated SQL: SELECT * FROM users WHERE %s\n", whereClause)
	fmt.Printf("📝 Parameters: %v\n", params)

	fmt.Println("\n🎉 Basic usage example completed!")
}
