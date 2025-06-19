package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jmag-ic/gosura/pkg/hooks"
	"github.com/jmag-ic/gosura/pkg/inspector"
)

const (
	POSTGRES_USER     = "postgres"
	POSTGRES_PASSWORD = "postgres"
	POSTGRES_HOST     = "localhost"
	POSTGRES_PORT     = "5432"
	POSTGRES_DB       = "postgres"
	TEST_TABLE        = "gosura_pgx_test"
)

func main() {
	// Database connection parameters
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)

	ctx := context.Background()

	// Connect to database
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer conn.Close(ctx)

	// Create test table
	if err := createTestTable(ctx, conn); err != nil {
		log.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	if err := insertTestData(ctx, conn); err != nil {
		log.Fatalf("Failed to insert test data: %v", err)
	}

	// Test all PostgreSQL operators
	successCount, failureCount, failedTests := run(conn)

	// Print summary
	printSummary(successCount, failureCount, failedTests)
}

func createTestTable(ctx context.Context, db *pgx.Conn) error {
	createTableSQL := fmt.Sprintf(`
	DROP TABLE IF EXISTS public.%s;
	CREATE TABLE public.%s (
		id serial NOT NULL,
		username text UNIQUE NOT NULL,
		name text NULL,
		age integer NULL,
		average numeric NULL,
		is_active boolean NULL,
		tags text[] NULL,
		metadata jsonb NULL,
		ip cidr NULL
	);`, TEST_TABLE, TEST_TABLE)

	_, err := db.Exec(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	fmt.Printf("✅ Test table '%s' created successfully\n", TEST_TABLE)
	return nil
}

func insertTestData(ctx context.Context, db *pgx.Conn) error {
	insertSQL := fmt.Sprintf(`
	INSERT INTO public.%s (username, name, age, average, is_active, tags, metadata, ip) VALUES
	('johndoe', 'John Doe', 30, 85.5, true, ARRAY['developer', 'golang'], '{"role": "admin", "department": "engineering", "skills": ["go", "postgresql"]}', '192.168.1.0/24'),
	('janesmith', 'Jane Smith', 25, 92.3, true, ARRAY['designer', 'ui'], '{"role": "user", "department": "design", "preferences": {"theme": "dark"}}', '10.0.0.0/24'),
	('bobjohnson', 'Bob Johnson', 35, 78.9, false, ARRAY['manager'], '{"role": "manager", "department": "sales", "reports": ["team1", "team2"]}', '172.16.0.0/24'),
	('alicebrown', 'Alice Brown', 28, 88.1, true, ARRAY['analyst', 'data'], '{"role": "analyst", "department": "data", "tools": ["python", "sql"]}', '203.0.113.0/24'),
	('charliewilson', 'Charlie Wilson', 42, 95.7, true, ARRAY['architect'], '{"role": "architect", "department": "engineering", "certifications": ["aws", "kubernetes"]}', '198.51.100.0/24');`,
		TEST_TABLE)

	_, err := db.Exec(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}
	fmt.Println("✅ Test data inserted successfully")
	return nil
}

func run(conn *pgx.Conn) (int, int, []string) {
	fmt.Println("\n🧪 Testing PostgreSQL Operators with NewPostgresParseHookConfig")
	fmt.Println("================================================================")

	// Create inspector and hook
	inspector := &inspector.HasuraInspector{}

	// Test counters
	var successCount, failureCount int
	var failedTests []string

	// Postgres hook config should be instantiated once and reused
	postgresHookConfig := hooks.NewPostgresParseHookConfig()

	// All test cases for PostgreSQL operators
	testCases := []struct {
		category          string
		name              string
		filter            string
		description       string
		expectedUsernames []string
	}{
		// JSONB Operations
		{
			category:          "JSONB",
			name:              "_contains",
			filter:            `{"where":{"metadata":{"_contains":{"role":"admin"}}}}`,
			description:       "Find records where metadata contains role=admin",
			expectedUsernames: []string{"johndoe"},
		},
		{
			category:          "JSONB",
			name:              "_contained_in (should match)",
			filter:            `{"where":{"metadata":{"_contained_in":{"role":"user","department":"design","preferences":{"theme":"dark"}}}}}`,
			description:       "Find records where metadata is contained in the specified object (exact match)",
			expectedUsernames: []string{"janesmith"},
		},
		{
			category:          "JSONB",
			name:              "_contained_in (should not match)",
			filter:            `{"where":{"metadata":{"_contained_in":{"role":"user","department":"design"}}}}`,
			description:       "Find records where metadata is contained in the specified object (missing preferences field)",
			expectedUsernames: []string{},
		},
		{
			category:          "JSONB",
			name:              "_has_key",
			filter:            `{"where":{"metadata":{"_has_key":"role"}}}`,
			description:       "Find records where metadata has the 'role' key",
			expectedUsernames: []string{"johndoe", "janesmith", "bobjohnson", "alicebrown", "charliewilson"},
		},
		{
			category:          "JSONB",
			name:              "_has_keys_any",
			filter:            `{"where":{"metadata":{"_has_keys_any":["role","department"]}}}`,
			description:       "Find records where metadata has any of the specified keys",
			expectedUsernames: []string{"johndoe", "janesmith", "bobjohnson", "alicebrown", "charliewilson"},
		},
		{
			category:          "JSONB",
			name:              "_has_keys_all",
			filter:            `{"where":{"metadata":{"_has_keys_all":["role","department"]}}}`,
			description:       "Find records where metadata has all of the specified keys",
			expectedUsernames: []string{"johndoe", "janesmith", "bobjohnson", "alicebrown", "charliewilson"},
		},
		{
			category:          "JSONB",
			name:              "nested _contains",
			filter:            `{"where":{"metadata":{"_contains":{"department":"engineering"}}}}`,
			description:       "Find records where metadata contains department=engineering",
			expectedUsernames: []string{"johndoe", "charliewilson"},
		},
		{
			category:          "JSONB",
			name:              "array _contains",
			filter:            `{"where":{"metadata":{"_contains":{"skills":["go","postgresql"]}}}}`,
			description:       "Find records where metadata contains the skills array",
			expectedUsernames: []string{"johndoe"},
		},
		{
			category:          "JSONB",
			name:              "complex _contains",
			filter:            `{"where":{"metadata":{"_contains":{"role":"architect","certifications":["aws","kubernetes"]}}}}`,
			description:       "Find records with complex nested JSONB structure",
			expectedUsernames: []string{"charliewilson"},
		},
		{
			category:          "JSONB",
			name:              "_has_key with _and",
			filter:            `{"where":{"_and":[{"metadata":{"_has_key":"role"}},{"age":{"_gt":30}}]}}`,
			description:       "Find records with role key AND age > 30",
			expectedUsernames: []string{"bobjohnson", "charliewilson"},
		},
		{
			category:          "JSONB",
			name:              "_contains with _or",
			filter:            `{"where":{"_or":[{"metadata":{"_contains":{"role":"admin"}}},{"metadata":{"_contains":{"role":"manager"}}}]}}`,
			description:       "Find records with role admin OR manager",
			expectedUsernames: []string{"johndoe", "bobjohnson"},
		},
		{
			category:          "JSONB",
			name:              "_has_key with specific key",
			filter:            `{"where":{"metadata":{"_has_key":"certifications"}}}`,
			description:       "Find records that have certifications key",
			expectedUsernames: []string{"charliewilson"},
		},
		{
			category:          "JSONB",
			name:              "_contains with preferences",
			filter:            `{"where":{"metadata":{"_contains":{"preferences":{"theme":"dark"}}}}}`,
			description:       "Find records with dark theme preference",
			expectedUsernames: []string{"janesmith"},
		},

		// Basic Comparisons
		{
			category:          "Basic",
			name:              "_eq",
			filter:            `{"where":{"age":{"_eq":30}}}`,
			description:       "Find records where age equals 30",
			expectedUsernames: []string{"johndoe"},
		},
		{
			category:          "Basic",
			name:              "_neq",
			filter:            `{"where":{"age":{"_neq":30}}}`,
			description:       "Find records where age is not equal to 30",
			expectedUsernames: []string{"janesmith", "bobjohnson", "alicebrown", "charliewilson"},
		},
		{
			category:          "Basic",
			name:              "_gt",
			filter:            `{"where":{"age":{"_gt":30}}}`,
			description:       "Find records where age is greater than 30",
			expectedUsernames: []string{"bobjohnson", "charliewilson"},
		},
		{
			category:          "Basic",
			name:              "_lt",
			filter:            `{"where":{"age":{"_lt":30}}}`,
			description:       "Find records where age is less than 30",
			expectedUsernames: []string{"janesmith", "alicebrown"},
		},
		{
			category:          "Basic",
			name:              "_gte",
			filter:            `{"where":{"age":{"_gte":30}}}`,
			description:       "Find records where age is greater than or equal to 30",
			expectedUsernames: []string{"johndoe", "bobjohnson", "charliewilson"},
		},
		{
			category:          "Basic",
			name:              "_lte",
			filter:            `{"where":{"age":{"_lte":30}}}`,
			description:       "Find records where age is less than or equal to 30",
			expectedUsernames: []string{"johndoe", "janesmith", "alicebrown"},
		},

		// Null checks
		{
			category:          "Null",
			name:              "_is_null true",
			filter:            `{"where":{"average":{"_is_null":true}}}`,
			description:       "Find records where average is null",
			expectedUsernames: []string{},
		},
		{
			category:          "Null",
			name:              "_is_null false",
			filter:            `{"where":{"average":{"_is_null":false}}}`,
			description:       "Find records where average is not null",
			expectedUsernames: []string{"johndoe", "janesmith", "bobjohnson", "alicebrown", "charliewilson"},
		},

		// IN / NOT IN
		{
			category:          "IN",
			name:              "_in",
			filter:            `{"where":{"age":{"_in":[25, 30, 35]}}}`,
			description:       "Find records where age is in [25, 30, 35]",
			expectedUsernames: []string{"janesmith", "johndoe", "bobjohnson"},
		},
		{
			category:          "IN",
			name:              "_nin",
			filter:            `{"where":{"age":{"_nin":[25, 30, 35]}}}`,
			description:       "Find records where age is not in [25, 30, 35]",
			expectedUsernames: []string{"alicebrown", "charliewilson"},
		},

		// LIKE operators
		{
			category:          "LIKE",
			name:              "_like",
			filter:            `{"where":{"name":{"_like":"%John%"}}}`,
			description:       "Find records where name contains 'John'",
			expectedUsernames: []string{"johndoe", "bobjohnson"},
		},
		{
			category:          "LIKE",
			name:              "_nlike",
			filter:            `{"where":{"name":{"_nlike":"%John%"}}}`,
			description:       "Find records where name does not contain 'John'",
			expectedUsernames: []string{"janesmith", "alicebrown", "charliewilson"},
		},
		{
			category:          "LIKE",
			name:              "_ilike",
			filter:            `{"where":{"name":{"_ilike":"%jane%"}}}`,
			description:       "Find records where name contains 'jane' (case insensitive)",
			expectedUsernames: []string{"janesmith"},
		},
		{
			category:          "LIKE",
			name:              "_nilike",
			filter:            `{"where":{"name":{"_nilike":"%jane%"}}}`,
			description:       "Find records where name does not contain 'jane' (case insensitive)",
			expectedUsernames: []string{"johndoe", "bobjohnson", "alicebrown", "charliewilson"},
		},

		// Regex operators
		{
			category:          "Regex",
			name:              "_similar",
			filter:            `{"where":{"name":{"_similar":"%John%"}}}`,
			description:       "Find records where name is similar to '%John%'",
			expectedUsernames: []string{"johndoe", "bobjohnson"},
		},
		{
			category:          "Regex",
			name:              "_nsimilar",
			filter:            `{"where":{"name":{"_nsimilar":"%John%"}}}`,
			description:       "Find records where name is not similar to '%John%'",
			expectedUsernames: []string{"janesmith", "alicebrown", "charliewilson"},
		},
		{
			category:          "Regex",
			name:              "_regex",
			filter:            `{"where":{"name":{"_regex":"John"}}}`,
			description:       "Find records where name matches regex 'John'",
			expectedUsernames: []string{"johndoe", "bobjohnson"},
		},
		{
			category:          "Regex",
			name:              "_nregex",
			filter:            `{"where":{"name":{"_nregex":"John"}}}`,
			description:       "Find records where name does not match regex 'John'",
			expectedUsernames: []string{"janesmith", "alicebrown", "charliewilson"},
		},
		{
			category:          "Regex",
			name:              "_iregex",
			filter:            `{"where":{"name":{"_iregex":"jane"}}}`,
			description:       "Find records where name matches regex 'jane' (case insensitive)",
			expectedUsernames: []string{"janesmith"},
		},
		{
			category:          "Regex",
			name:              "_niregex",
			filter:            `{"where":{"name":{"_niregex":"jane"}}}`,
			description:       "Find records where name does not match regex 'jane' (case insensitive)",
			expectedUsernames: []string{"johndoe", "bobjohnson", "alicebrown", "charliewilson"},
		},

		// Complex combinations
		{
			category:          "Complex",
			name:              "_and",
			filter:            `{"where":{"_and":[{"age":{"_gt":25}},{"age":{"_lt":40}}]}}`,
			description:       "Find records where age is between 25 and 40",
			expectedUsernames: []string{"johndoe", "bobjohnson", "alicebrown"},
		},
		{
			category:          "Complex",
			name:              "_or",
			filter:            `{"where":{"_or":[{"age":{"_eq":25}},{"age":{"_eq":42}}]}}`,
			description:       "Find records where age is 25 or 42",
			expectedUsernames: []string{"janesmith", "charliewilson"},
		},
		{
			category:          "Complex",
			name:              "_not",
			filter:            `{"where":{"_not":{"age":{"_eq":30}}}}`,
			description:       "Find records where age is not 30",
			expectedUsernames: []string{"janesmith", "bobjohnson", "alicebrown", "charliewilson"},
		},
		{
			category:          "Complex",
			name:              "AND/OR combination",
			filter:            `{"where":{"_and":[{"age":{"_gte":25}},{"_or":[{"name":{"_like":"%John%"}},{"name":{"_like":"%Jane%"}}]}]}}`,
			description:       "Find records where age >= 25 AND (name contains 'John' OR 'Jane')",
			expectedUsernames: []string{"johndoe", "janesmith", "bobjohnson"},
		},
	}

	var currentCategory string
	for _, tc := range testCases {
		// Print category header when it changes
		if currentCategory != tc.category {
			currentCategory = tc.category
			fmt.Printf("\n📂 Category: %s\n", currentCategory)
			fmt.Println(strings.Repeat("-", 50))
		}

		fmt.Printf("\n📋 Test: %s\n", tc.name)
		fmt.Printf("   Description: %s\n", tc.description)
		fmt.Printf("   Filter: %s\n", tc.filter)
		fmt.Printf("   Expected usernames: %v\n", tc.expectedUsernames)

		// Create sqlParseHook with PostgreSQL config
		sqlParseHook := hooks.NewSQLParseHook(postgresHookConfig)

		// Process the filter
		err := inspector.Inspect(context.Background(), tc.filter, sqlParseHook)
		if err != nil {
			fmt.Printf("   ❌ Error processing filter: %v\n", err)
			failureCount++
			failedTests = append(failedTests, fmt.Sprintf("%s: %s", tc.category, tc.name))
			continue
		}

		// Get the generated SQL
		whereClause, params := sqlParseHook.GetWhereClause()
		sqlQuery := fmt.Sprintf("SELECT id, username, name, age, metadata FROM public.%s WHERE %s", TEST_TABLE, whereClause)

		fmt.Printf("   Generated SQL: %s\n", sqlQuery)
		fmt.Printf("   Parameters: %v\n", params)

		// Execute the query
		rows, err := conn.Query(context.Background(), sqlQuery, params...)
		if err != nil {
			fmt.Printf("   ❌ Query execution error: %v\n", err)
			failureCount++
			failedTests = append(failedTests, fmt.Sprintf("%s: %s", tc.category, tc.name))
			continue
		}

		// Process results
		var results []map[string]any
		for rows.Next() {
			var id int
			var username string
			var name string
			var age int
			var metadata []byte

			err := rows.Scan(&id, &username, &name, &age, &metadata)
			if err != nil {
				fmt.Printf("   ❌ Row scan error: %v\n", err)
				continue
			}

			results = append(results, map[string]any{
				"id":       id,
				"username": username,
				"name":     name,
				"age":      age,
				"metadata": string(metadata),
			})
		}
		rows.Close()

		// Extract actual usernames from results
		var actualUsernames []string
		for _, result := range results {
			actualUsernames = append(actualUsernames, result["username"].(string))
		}

		fmt.Printf("   ✅ Found %d matching records with usernames: %v\n", len(results), actualUsernames)

		// Validate results
		if validateResults(actualUsernames, tc.expectedUsernames) {
			fmt.Printf("   🎯 Validation: PASSED - Results match expectations\n")
			successCount++
		} else {
			fmt.Printf("   ❌ Validation: FAILED - Expected usernames %v but got %v\n", tc.expectedUsernames, actualUsernames)
			failureCount++
			failedTests = append(failedTests, fmt.Sprintf("%s: %s", tc.category, tc.name))
		}

		// Show detailed results
		for _, result := range results {
			fmt.Printf("      - ID: %d, Username: %s, Name: %s, Age: %d, Metadata: %s\n",
				result["id"], result["username"], result["name"], result["age"], result["metadata"])
		}
	}

	return successCount, failureCount, failedTests
}

// printSummary displays a summary of all test results
func printSummary(successCount, failureCount int, failedTests []string) {
	totalTests := successCount + failureCount

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("🌍 TEST EXECUTION SUMMARY")
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("Total Tests: %d\n", totalTests)
	fmt.Printf("✅ Successful: %d\n", successCount)
	fmt.Printf("❌ Failed: %d\n", failureCount)

	successRate := float64(successCount) / float64(totalTests) * 100
	fmt.Printf("📈 Success Rate: %.1f%%\n", successRate)

	if failureCount > 0 {
		fmt.Printf("\n❌ Failed Tests:\n")
		for _, testName := range failedTests {
			fmt.Printf("   - %s\n", testName)
		}
	} else {
		fmt.Printf("\n🎉 All tests passed successfully!\n")
	}

	fmt.Println(strings.Repeat("=", 80))
}

// validateResults compares actual results with expected results
func validateResults(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}

	// Create maps for comparison
	actualMap := make(map[string]bool)
	expectedMap := make(map[string]bool)

	for _, username := range actual {
		actualMap[username] = true
	}

	for _, username := range expected {
		expectedMap[username] = true
	}

	// Compare maps
	for username := range actualMap {
		if !expectedMap[username] {
			return false
		}
	}

	for username := range expectedMap {
		if !actualMap[username] {
			return false
		}
	}

	return true
}
