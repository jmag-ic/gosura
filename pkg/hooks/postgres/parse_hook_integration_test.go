//go:build integration
// +build integration

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jmag-ic/gosura/pkg/hooks/sql"
	"github.com/jmag-ic/gosura/pkg/inspector"
	"github.com/stretchr/testify/require"
)

const TABLE_NAME = "gosura_pgintegration_test"

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
	);`, TABLE_NAME, TABLE_NAME)

	_, err := db.Exec(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	return nil
}

func dropTestTable(ctx context.Context, db *pgx.Conn) error {
	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS public.%s", TABLE_NAME)
	_, err := db.Exec(ctx, dropTableSQL)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
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
		TABLE_NAME)

	_, err := db.Exec(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}
	return nil
}

func executeQuery(ctx context.Context, db *pgx.Conn, query string, params []any) ([]string, error) {
	rows, err := db.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	usernames := []string{}
	for rows.Next() {
		var username string
		err := rows.Scan(&username)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		usernames = append(usernames, username)
	}
	return usernames, nil
}

func TestSQLParseHook_PostgresIntegration(t *testing.T) {
	ctx := context.Background()
	dbURL := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

	conn, err := pgx.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	require.NoError(t, createTestTable(ctx, conn))
	defer dropTestTable(ctx, conn)
	require.NoError(t, insertTestData(ctx, conn))

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

	hasuraInspector := &inspector.HasuraInspector{}
	postgresHookConfig := NewParseHookConfig()

	for _, c := range cases {

		sqlParseHook := sql.NewSQLParseHook(postgresHookConfig)

		t.Run(c.name, func(t *testing.T) {
			err := hasuraInspector.Inspect(ctx, c.filter, sqlParseHook)
			require.NoError(t, err)

			whereClause, params := sqlParseHook.GetWhereClause()
			sqlQuery := fmt.Sprintf("SELECT username FROM public.%s WHERE %s", TABLE_NAME, whereClause)

			actualUsernames, err := executeQuery(ctx, conn, sqlQuery, params)
			require.NoError(t, err)
			require.True(t, validateResults(actualUsernames, c.expectedUsernames))
		})
	}
}

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

func TestSQLParseHook_AggregateIntegration(t *testing.T) {
	ctx := context.Background()
	dbURL := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

	conn, err := pgx.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	require.NoError(t, createTestTable(ctx, conn))
	defer dropTestTable(ctx, conn)
	require.NoError(t, insertTestData(ctx, conn))

	cases := []struct {
		name            string
		filter          string
		description     string
		validateResults func(t *testing.T, rows pgx.Rows)
	}{
		{
			name:        "Count all records",
			filter:      `{"aggregate":{"count":"*"}}`,
			description: "Count all records in the table",
			validateResults: func(t *testing.T, rows pgx.Rows) {
				require.True(t, rows.Next())
				var count int64
				err := rows.Scan(&count)
				require.NoError(t, err)
				require.Equal(t, int64(5), count)
			},
		},
		{
			name:        "Sum of ages",
			filter:      `{"aggregate":{"sum":"age"}}`,
			description: "Sum all ages (30+25+35+28+42 = 160)",
			validateResults: func(t *testing.T, rows pgx.Rows) {
				require.True(t, rows.Next())
				var sum int64
				err := rows.Scan(&sum)
				require.NoError(t, err)
				require.Equal(t, int64(160), sum)
			},
		},
		{
			name:        "Average age",
			filter:      `{"aggregate":{"avg":"age"}}`,
			description: "Average age (160/5 = 32)",
			validateResults: func(t *testing.T, rows pgx.Rows) {
				require.True(t, rows.Next())
				var avg float64
				err := rows.Scan(&avg)
				require.NoError(t, err)
				require.InDelta(t, 32.0, avg, 0.01)
			},
		},
		{
			name:        "Min and max age",
			filter:      `{"aggregate":{"min":"age","max":"age"}}`,
			description: "Min age (25) and Max age (42)",
			validateResults: func(t *testing.T, rows pgx.Rows) {
				require.True(t, rows.Next())
				var min, max int64
				err := rows.Scan(&min, &max)
				require.NoError(t, err)
				require.Equal(t, int64(25), min)
				require.Equal(t, int64(42), max)
			},
		},
		{
			name:        "Multiple aggregates",
			filter:      `{"aggregate":{"count":"*","sum":"age","avg":"average"}}`,
			description: "Count, sum of ages, and average of averages",
			validateResults: func(t *testing.T, rows pgx.Rows) {
				require.True(t, rows.Next())
				var count, sumAge int64
				var avgAverage float64
				err := rows.Scan(&count, &sumAge, &avgAverage)
				require.NoError(t, err)
				require.Equal(t, int64(5), count)
				require.Equal(t, int64(160), sumAge)
				require.InDelta(t, 88.1, avgAverage, 0.1)
			},
		},
		{
			name:        "Aggregate with WHERE clause",
			filter:      `{"where":{"is_active":{"_eq":true}},"aggregate":{"count":"*","avg":"age"}}`,
			description: "Count and average age of active users only",
			validateResults: func(t *testing.T, rows pgx.Rows) {
				require.True(t, rows.Next())
				var count int64
				var avgAge float64
				err := rows.Scan(&count, &avgAge)
				require.NoError(t, err)
				require.Equal(t, int64(4), count)       // 4 active users
				require.InDelta(t, 31.25, avgAge, 0.01) // (30+25+28+42)/4
			},
		},
		{
			name:        "Count distinct with option",
			filter:      `{"aggregate":{"count":{"field":"is_active","distinct":true}}}`,
			description: "Count distinct is_active values (true and false = 2)",
			validateResults: func(t *testing.T, rows pgx.Rows) {
				require.True(t, rows.Next())
				var count int64
				err := rows.Scan(&count)
				require.NoError(t, err)
				require.Equal(t, int64(2), count)
			},
		},
		{
			name:        "PostgreSQL-specific: STDDEV",
			filter:      `{"aggregate":{"stddev":"age"}}`,
			description: "Standard deviation of ages",
			validateResults: func(t *testing.T, rows pgx.Rows) {
				require.True(t, rows.Next())
				var stddev float64
				err := rows.Scan(&stddev)
				require.NoError(t, err)
				require.Greater(t, stddev, 0.0)
			},
		},
		{
			name:        "PostgreSQL-specific: VARIANCE",
			filter:      `{"aggregate":{"variance":"age"}}`,
			description: "Variance of ages",
			validateResults: func(t *testing.T, rows pgx.Rows) {
				require.True(t, rows.Next())
				var variance float64
				err := rows.Scan(&variance)
				require.NoError(t, err)
				require.Greater(t, variance, 0.0)
			},
		},
	}

	hasuraInspector := &inspector.HasuraInspector{}
	postgresHookConfig := NewParseHookConfig()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sqlParseHook := sql.NewSQLParseHook(postgresHookConfig)

			err := hasuraInspector.Inspect(ctx, c.filter, sqlParseHook)
			require.NoError(t, err)

			aggregates := sqlParseHook.GetAggregates()
			selectClause := strings.Join(aggregates, ", ")
			if selectClause == "" {
				selectClause = "*"
			}
			whereClause, params := sqlParseHook.GetWhereClause()

			var sqlQuery string
			if whereClause != "" {
				sqlQuery = fmt.Sprintf("SELECT %s FROM public.%s WHERE %s", selectClause, TABLE_NAME, whereClause)
			} else {
				sqlQuery = fmt.Sprintf("SELECT %s FROM public.%s", selectClause, TABLE_NAME)
			}

			rows, err := conn.Query(ctx, sqlQuery, params...)
			require.NoError(t, err)
			defer rows.Close()

			c.validateResults(t, rows)
		})
	}
}
