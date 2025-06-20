# Gosura Filter Inspector

[![Go Report Card](https://goreportcard.com/badge/github.com/jmag-ic/gosura)](https://goreportcard.com/report/github.com/jmag-ic/gosura)
[![Go Version](https://img.shields.io/github/go-mod/go-version/jmag-ic/gosura)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Gosura** is a **PostgreSQL-first Go library** for converting Hasura-style GraphQL filters into SQL clauses with extensible hook-based architecture.

---

## 🚀 Features

- **Hasura Filter Parsing** – Convert GraphQL-style filters into SQL `WHERE` clauses  
- **PostgreSQL Support** – Compatibility with PostgreSQL operators
- **Extensible Architecture** – Plugin-based hook system for custom filter processing
- **Comprehensive Testing** – Unit tests and integration tests

---

## 🏗️ Architecture Overview

Gosura follows an extensible three-component design:

### 1. Inspector
The `HasuraInspector` is the core component responsible for parsing and processing Hasura-style filters. It:
- Inspects the structure of the filter JSON
- Manages the overall inspection workflow
- Delegates each filter node processing to registered hooks

### 2. Filter
Filters are defined in JSON using Hasura's GraphQL filter syntax:
```json
{
  "where": {
    "_and": [
      { "age": { "_gte": 25 } },
      { "name": { "_like": "%John%" } }
    ]
  }
}
```

### 3. Hooks
Hooks are responsible for processing specific parts of the filter. Each hook:
- Implements the `FilterHook` interface:
```go
type FilterHook interface {
	OnComparison(ctx context.Context, field string, operator string, value gjson.Result, path []string) error
	OnNestedNodeStart(ctx context.Context, field string, node gjson.Result, src string, path []string)
	OnNestedNodeEnd(ctx context.Context, field string, node gjson.Result, src string, path []string)
	OnLogicalGroupStart(ctx context.Context, operator string, node gjson.Result, path []string) error
	OnLogicalGroupEnd(ctx context.Context, operator string, node gjson.Result, path []string)
	OnOrderBy(ctx context.Context, field string, direction string, path []string)
}
```
- Processes each filter node according to its logic (e.g., SQL generation)

---

### 🧩 How They Work Together

```go
// 1. Create the inspector
inspector := &inspector.HasuraInspector{}

// 2. Create a PostgreSQL-compatible hook
sqlParseHook := sql.NewSQLParseHook(postgres.NewParseHookConfig())

// 3. Process the filter JSON
err := inspector.Inspect(context.Background(), filterJSON, sqlParseHook)

// 4. Retrieve the resulting SQL clause and parameters
whereClause, params := sqlParseHook.GetWhereClause()
// e.g. "age >= $1 AND name LIKE $2" with params [25, "%John%"]
```

**Why it matters:**
- 🔄 Easily switch databases by swapping hooks  
- 🔧 Extend functionality with custom operators  
- 🧪 Unit-test each hook independently  
- 🧼 Maintain a clean separation between filter inspection and filter processing (SQL generation)

---

## 🎯 Quick Start

```go
package main

import (
    "context"
    "fmt"

    "github.com/jmag-ic/gosura/pkg/hooks/postgres"
    "github.com/jmag-ic/gosura/pkg/hooks/sql"
    "github.com/jmag-ic/gosura/pkg/inspector"
)

func main() {
    filterJSON := `{
        "where": {
            "_and": [
                { "age": { "_gte": 25 } },
                { "name": { "_like": "%John%" } }
            ]
        }
    }`

    inspector := &inspector.HasuraInspector{}
    sqlParseHook := sql.NewSQLParseHook(postgres.NewParseHookConfig())

    if err := inspector.Inspect(context.Background(), filterJSON, sqlParseHook); err != nil {
        panic(err)
    }

    whereClause, params := sqlParseHook.GetWhereClause()

    fmt.Printf("SQL: SELECT * FROM users WHERE %s\n", whereClause)
    fmt.Printf("Parameters: %v\n", params)
}
```

### Run Examples

```bash
# Basic usage example
go run examples/basic-usage/main.go

# PGX integration example (requires PostgreSQL)
go run examples/pgx-integration/main.go
```

---

## 🔧 Supported Operators

### Basic Comparisons
- `_eq`, `_neq`, `_gt`, `_lt`, `_gte`, `_lte`

### Null Checks
- `_is_null`

### IN Operations
- `_in`, `_nin`

### Pattern Matching
- `_like`, `_nlike`, `_ilike`, `_nilike`

### Regular Expressions
- `_regex`, `_nregex`, `_iregex`, `_niregex`, `_similar`, `_nsimilar`

### JSONB Operations (PostgreSQL)
- `_contains` (@>)
- `_contained_in` (<@)
- `_has_key` (?)
- `_has_keys_any` (?|)
- `_has_keys_all` (?&)

### Logical Operators
- `_and`, `_or`, `_not`

---

## 📚 Examples

### Basic Equality
```go
filter := `{"where":{"name":{"_eq":"John"}}}`
// → "name = $1" with ["John"]
```

### JSONB Containment
```go
filter := `{"where":{"metadata":{"_contains":{"role":"admin"}}}}`
// → "metadata @> $1" with ["{\"role\": \"admin\"}"]
```

### Logical Combinations
```go
filter := `{
  "where": {
    "_and": [
      { "age": { "_gte": 25 } },
      {
        "_or": [
          { "name": { "_like": "%John%" } },
          { "name": { "_like": "%Jane%" } }
        ]
      },
      { "metadata": { "_has_key": "role" } }
    ]
  }
}`
// → "age >= $1 AND (name LIKE $2 OR name LIKE $3) AND metadata ? $4"
```

---

## 🧪 Testing

Run the full test suite:

```bash
# Run all tests
go test ./...

# With coverage
go test -cover ./...

# Run integration tests (requires PostgreSQL)
go test -tags=integration ./pkg/hooks/postgres/

# Run examples
go run examples/basic-usage/main.go
go run examples/pgx-integration/main.go
```

### Integration Testing

The project includes comprehensive integration tests that:
- Create real PostgreSQL tables
- Insert test data
- Execute actual queries using PGX
- Validate results against expected outcomes

To run integration tests, ensure you have PostgreSQL running with default credentials:
- Host: `localhost:5432`
- User: `postgres`
- Password: `postgres`
- Database: `postgres`

---

## 📁 Project Structure

```

## 🔗 Dependencies

- **Go 1.24+** - Modern Go features and performance
- **PGX v5** - High-performance PostgreSQL driver
- **GJSON** - Fast JSON parsing
- **Testify** - Testing utilities

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Inspired by [Hasura](https://hasura.io) filtering capabilities  
- Built with Go’s standard library  
- Powered by [tidwall/gjson](https://github.com/tidwall/gjson) library