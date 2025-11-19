package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jmag-ic/gosura/pkg/hooks/postgres"
	"github.com/jmag-ic/gosura/pkg/hooks/sql"
	"github.com/jmag-ic/gosura/pkg/inspector"
)

const (
	POSTGRES_USER     = "postgres"
	POSTGRES_PASSWORD = "postgres"
	POSTGRES_HOST     = "localhost"
	POSTGRES_PORT     = "5432"
	POSTGRES_DB       = "postgres"
	TABLE_NAME        = "public.gosura_pgx"
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

	// Create table
	if err := createTable(ctx, conn); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Insert example data
	if err := insertData(ctx, conn); err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	// Execute cases
	successCount, failureCount, failedCases := run(conn)

	// Print summary
	printSummary(successCount, failureCount, failedCases)

	// Drop table
	if err := dropTable(ctx, conn); err != nil {
		log.Fatalf("Failed to drop table: %v", err)
	}
}

func createTable(ctx context.Context, db *pgx.Conn) error {
	createTableSQL := fmt.Sprintf(`
	DROP TABLE IF EXISTS %s;
	CREATE TABLE %s (
		id serial NOT NULL,
		username text UNIQUE NOT NULL,
		name text NULL,
		age integer NULL,
		average numeric NULL,
		is_active boolean NULL,
		tags text[] NULL,
		metadata jsonb NULL,
		ip cidr NULL
	);`, TABLE_NAME, TABLE_NAME)

	_, err := db.Exec(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	fmt.Printf("✅ Table '%s' created successfully\n", TABLE_NAME)
	return nil
}

func dropTable(ctx context.Context, db *pgx.Conn) error {
	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", TABLE_NAME)
	_, err := db.Exec(ctx, dropTableSQL)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
	return nil
}

func insertData(ctx context.Context, db *pgx.Conn) error {
	insertSQL := fmt.Sprintf(`
	INSERT INTO %s (username, name, age, average, is_active, tags, metadata, ip) VALUES
	('johndoe', 'John Doe', 30, 85.5, true, ARRAY['developer', 'golang'], '{"role": "admin", "department": "engineering", "skills": ["go", "postgresql"]}', '192.168.1.0/24'),
	('janesmith', 'Jane Smith', 25, 92.3, true, ARRAY['designer', 'ui'], '{"role": "user", "department": "design", "preferences": {"theme": "dark"}}', '10.0.0.0/24'),
	('bobjohnson', 'Bob Johnson', 35, 78.9, false, ARRAY['manager'], '{"role": "manager", "department": "sales", "reports": ["team1", "team2"]}', '172.16.0.0/24'),
	('alicebrown', 'Alice Brown', 28, 88.1, true, ARRAY['analyst', 'data'], '{"role": "analyst", "department": "data", "tools": ["python", "sql"]}', '203.0.113.0/24'),
	('charliewilson', 'Charlie Wilson', 42, 95.7, true, ARRAY['architect'], '{"role": "architect", "department": "engineering", "certifications": ["aws", "kubernetes"]}', '198.51.100.0/24');`,
		TABLE_NAME)

	_, err := db.Exec(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}
	fmt.Println("✅ Data inserted successfully")
	return nil
}

func run(conn *pgx.Conn) (int, int, []string) {
	fmt.Println("\n🧪 PostgreSQL Operators with NewPostgresParseHookConfig")
	fmt.Println("================================================================")

	// Create inspector and hook
	inspector := &inspector.HasuraInspector{}

	// Counters
	var successCount, failureCount int
	var failedCases []string

	// Postgres hook config should be instantiated once and reused
	postgresHookConfig := postgres.NewParseHookConfig()

	// All cases for PostgreSQL operators
	cases := []struct {
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
	for _, c := range cases {
		// Print category header when it changes
		if currentCategory != c.category {
			currentCategory = c.category
			fmt.Printf("\n📂 Category: %s\n", currentCategory)
			fmt.Println(strings.Repeat("-", 50))
		}

		fmt.Printf("\n📋 Case: %s\n", c.name)
		fmt.Printf("   Description: %s\n", c.description)
		fmt.Printf("   Filter: %s\n", c.filter)
		fmt.Printf("   Expected usernames: %v\n", c.expectedUsernames)

		// Create SQL filter with PostgreSQL config
		filter := sql.NewSQLFilter(postgresHookConfig)

		// Process the filter
		err := inspector.Inspect(context.Background(), c.filter, filter)
		if err != nil {
			fmt.Printf("   ❌ Error processing filter: %v\n", err)
			failureCount++
			failedCases = append(failedCases, fmt.Sprintf("%s: %s", c.category, c.name))
			continue
		}

		// Get the generated SQL
		queryBuilder := filter.GetQueryBuilder()
		sqlQuery := queryBuilder.Build(TABLE_NAME, "id", "username", "name", "age", "metadata")

		fmt.Printf("   Generated SQL: %s\n", sqlQuery)
		fmt.Printf("   Parameters: %v\n", queryBuilder.Params)

		// Execute the query
		rows, err := conn.Query(context.Background(), sqlQuery, queryBuilder.Params...)
		if err != nil {
			fmt.Printf("   ❌ Query execution error: %v\n", err)
			failureCount++
			failedCases = append(failedCases, fmt.Sprintf("%s: %s", c.category, c.name))
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
		if validateResults(actualUsernames, c.expectedUsernames) {
			fmt.Printf("   🎯 Validation: PASSED - Results match expectations\n")
			successCount++
		} else {
			fmt.Printf("   ❌ Validation: FAILED - Expected usernames %v but got %v\n", c.expectedUsernames, actualUsernames)
			failureCount++
			failedCases = append(failedCases, fmt.Sprintf("%s: %s", c.category, c.name))
		}

		// Show detailed results
		for _, result := range results {
			fmt.Printf("      - ID: %d, Username: %s, Name: %s, Age: %d, Metadata: %s\n",
				result["id"], result["username"], result["name"], result["age"], result["metadata"])
		}
	}

	return successCount, failureCount, failedCases
}

// printSummary displays a summary of all cases
func printSummary(successCount, failureCount int, failedCases []string) {
	totalCases := successCount + failureCount

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("🌍 EXECUTION SUMMARY")
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("Total Cases: %d\n", totalCases)
	fmt.Printf("✅ Successful: %d\n", successCount)
	fmt.Printf("❌ Failed: %d\n", failureCount)

	successRate := float64(successCount) / float64(totalCases) * 100
	fmt.Printf("📈 Success Rate: %.1f%%\n", successRate)

	if failureCount > 0 {
		fmt.Printf("\n❌ Failed Cases:\n")
		for _, caseName := range failedCases {
			fmt.Printf("   - %s\n", caseName)
		}
	} else {
		fmt.Printf("\n🎉 All cases executed successfully!\n")
	}

	fmt.Println(strings.Repeat("=", 80))
}

// validateResults compares actual results with expected results
func validateResults(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}

	set := make(map[string]struct{}, len(actual))
	for _, v := range actual {
		set[v] = struct{}{}
	}

	for _, v := range expected {
		if _, ok := set[v]; !ok {
			return false
		}
	}

	return true
}
