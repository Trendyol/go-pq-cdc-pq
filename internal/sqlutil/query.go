package sqlutil

import (
	"fmt"
	"strings"
)

// BuildUpsertQuery builds an upsert SQL query from table name and data
// Example: table="users", data={"id": 1, "name": "John", "email": "john@example.com"}
// Returns: "INSERT INTO users (id, name, email) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET..."
func BuildUpsertQuery(table string, data map[string]any, primaryKey string) (string, []any) {
	columnNames := make([]string, 0, len(data))
	sqlPlaceholders := make([]string, 0, len(data))
	columnValues := make([]any, 0, len(data))

	placeholderIndex := 1
	// iterate over the data map and append the column names, values and placeholders to the respective slices
	for columnName, columnValue := range data {
		columnNames = append(columnNames, columnName)
		columnValues = append(columnValues, columnValue)
		sqlPlaceholders = append(sqlPlaceholders, fmt.Sprintf("$%d", placeholderIndex))
		placeholderIndex++
	}

	updateExpressions := buildUpdateExpressions(columnNames)

	query := fmt.Sprintf(`
        INSERT INTO %s (%s) VALUES (%s)
        ON CONFLICT (%s) DO UPDATE SET %s
    `,
		table,
		strings.Join(columnNames, ", "),
		strings.Join(sqlPlaceholders, ", "),
		primaryKey,
		strings.Join(updateExpressions, ", "),
	)

	return query, columnValues
}

// BuildDeleteQuery builds a delete SQL query from table name and old data
func BuildDeleteQuery(table string, old map[string]any, primaryKey string) (string, []any) {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, primaryKey)
	return query, []any{old[primaryKey]}
}

// buildUpdateExpressions creates update expressions for upsert queries
func buildUpdateExpressions(columnNames []string) []string {
	updateExpressions := make([]string, 0, len(columnNames))
	for _, columnName := range columnNames {
		updateExpressions = append(updateExpressions, fmt.Sprintf("%s = EXCLUDED.%s", columnName, columnName))
	}
	return updateExpressions
}

// CreateQueryBuilder returns a configured query builder function
func CreateQueryBuilder(primaryKey string) func(table string, data map[string]any) (string, []any) {
	return func(table string, data map[string]any) (string, []any) {
		return BuildUpsertQuery(table, data, primaryKey)
	}
}

// CreateDeleteQueryBuilder returns a configured delete query builder function
func CreateDeleteQueryBuilder(primaryKey string) func(table string, old map[string]any) (string, []any) {
	return func(table string, old map[string]any) (string, []any) {
		return BuildDeleteQuery(table, old, primaryKey)
	}
}
