package sqlutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    "users",
			expected: `"users"`,
		},
		{
			name:     "identifier with spaces",
			input:    "user name",
			expected: `"user name"`,
		},
		{
			name:     "identifier with double quotes",
			input:    `user"name`,
			expected: `"user""name"`,
		},
		{
			name:     "identifier with multiple double quotes",
			input:    `a"b"c`,
			expected: `"a""b""c"`,
		},
		{
			name:     "SQL injection attempt",
			input:    `users"; DROP TABLE users; --`,
			expected: `"users""; DROP TABLE users; --"`,
		},
		{
			name:     "reserved keyword",
			input:    "select",
			expected: `"select"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeIdentifier(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildUpsertQuery(t *testing.T) {
	data := map[string]any{
		"id":   1,
		"name": "test",
		"age":  25,
	}

	query, args := BuildUpsertQuery("users", data, "id")

	// Check that identifiers are properly quoted
	assert.Contains(t, query, `INSERT INTO "users"`)
	assert.Contains(t, query, `ON CONFLICT ("id") DO UPDATE SET`)
	assert.Len(t, args, 3)
	assert.Contains(t, args, 1)
	assert.Contains(t, args, "test")
	assert.Contains(t, args, 25)
}

func TestBuildUpsertQuery_SQLInjectionPrevention(t *testing.T) {
	// Test with malicious table name
	data := map[string]any{
		"id": 1,
	}

	query, _ := BuildUpsertQuery(`users"; DROP TABLE users; --`, data, "id")

	// The malicious table name should be properly quoted and escaped
	// The double quote in the malicious string is escaped by doubling it
	assert.Contains(t, query, `INSERT INTO "users""; DROP TABLE users; --"`)
	// The entire malicious string is now safely contained within the identifier quotes
	// so the DROP TABLE is part of the table name, not a separate SQL statement
}

func TestBuildDeleteQuery(t *testing.T) {
	oldData := map[string]any{
		"id":   1,
		"name": "test",
	}

	query, args := BuildDeleteQuery("users", oldData, "id")

	// Check that identifiers are properly quoted
	expectedQuery := `DELETE FROM "users" WHERE "id" = $1`
	assert.Equal(t, expectedQuery, query)
	assert.Equal(t, []any{1}, args)
}

func TestBuildDeleteQuery_SQLInjectionPrevention(t *testing.T) {
	oldData := map[string]any{
		"id": 1,
	}

	// Test with malicious primary key name
	query, _ := BuildDeleteQuery("users", oldData, `id" OR "1"="1`)

	// The malicious primary key should be properly quoted and escaped
	assert.Contains(t, query, `WHERE "id"" OR ""1""=""1"`)
}

func TestCreateQueryBuilder(t *testing.T) {
	queryBuilder := CreateQueryBuilder("id")
	assert.NotNil(t, queryBuilder)

	data := map[string]any{
		"id":   1,
		"name": "test",
	}

	query, args := queryBuilder("users", data)
	assert.Contains(t, query, `INSERT INTO "users"`)
	assert.Contains(t, query, `ON CONFLICT ("id") DO UPDATE SET`)
	assert.Len(t, args, 2)
}

func TestCreateDeleteQueryBuilder(t *testing.T) {
	deleteBuilder := CreateDeleteQueryBuilder("id")
	assert.NotNil(t, deleteBuilder)

	oldData := map[string]any{
		"id":   1,
		"name": "test",
	}

	query, args := deleteBuilder("users", oldData)
	expectedQuery := `DELETE FROM "users" WHERE "id" = $1`
	assert.Equal(t, expectedQuery, query)
	assert.Equal(t, []any{1}, args)
}
