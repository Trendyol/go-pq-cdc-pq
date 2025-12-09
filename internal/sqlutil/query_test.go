package sqlutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildUpsertQuery(t *testing.T) {
	data := map[string]any{
		"id":   1,
		"name": "test",
		"age":  25,
	}

	query, args := BuildUpsertQuery("users", data, "id")

	assert.Contains(t, query, "INSERT INTO users")
	assert.Contains(t, query, "ON CONFLICT (id) DO UPDATE SET")
	assert.Len(t, args, 3)
	assert.Contains(t, args, 1)
	assert.Contains(t, args, "test")
	assert.Contains(t, args, 25)
}

func TestBuildDeleteQuery(t *testing.T) {
	oldData := map[string]any{
		"id":   1,
		"name": "test",
	}

	query, args := BuildDeleteQuery("users", oldData, "id")

	expectedQuery := "DELETE FROM users WHERE id = $1"
	assert.Equal(t, expectedQuery, query)
	assert.Equal(t, []any{1}, args)
}

func TestCreateQueryBuilder(t *testing.T) {
	queryBuilder := CreateQueryBuilder("id")
	assert.NotNil(t, queryBuilder)

	data := map[string]any{
		"id":   1,
		"name": "test",
	}

	query, args := queryBuilder("users", data)
	assert.Contains(t, query, "INSERT INTO users")
	assert.Contains(t, query, "ON CONFLICT (id) DO UPDATE SET")
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
	expectedQuery := "DELETE FROM users WHERE id = $1"
	assert.Equal(t, expectedQuery, query)
	assert.Equal(t, []any{1}, args)
}
