package gopqcdcpq

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/stretchr/testify/assert"
)

func TestNewInsertMessage(t *testing.T) {
	now := time.Now()
	insertMsg := &format.Insert{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		Decoded: map[string]any{
			"id":   1,
			"name": "test",
			"age":  25,
		},
	}

	msg := NewInsertMessage(insertMsg)

	assert.NotNil(t, msg)
	assert.Equal(t, now, msg.EventTime)
	assert.Equal(t, "users", msg.TableName)
	assert.Equal(t, "public", msg.TableNamespace)
	assert.Nil(t, msg.OldData)
	assert.Equal(t, insertMsg.Decoded, msg.NewData)
	assert.Equal(t, InsertMessage, msg.Type)
	assert.True(t, msg.Type.IsInsert())
	assert.False(t, msg.Type.IsUpdate())
	assert.False(t, msg.Type.IsDelete())
}

func TestNewInsertMessageWithEmptyData(t *testing.T) {
	now := time.Now()
	insertMsg := &format.Insert{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		Decoded:        map[string]any{},
	}

	msg := NewInsertMessage(insertMsg)

	assert.NotNil(t, msg)
	assert.Equal(t, now, msg.EventTime)
	assert.Equal(t, "users", msg.TableName)
	assert.Equal(t, "public", msg.TableNamespace)
	assert.Nil(t, msg.OldData)
	assert.NotNil(t, msg.NewData)
	assert.Empty(t, msg.NewData)
	assert.Equal(t, InsertMessage, msg.Type)
}

func TestNewUpdateMessage(t *testing.T) {
	now := time.Now()
	updateMsg := &format.Update{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded: map[string]any{
			"id":   1,
			"name": "old",
			"age":  20,
		},
		NewDecoded: map[string]any{
			"id":   1,
			"name": "new",
			"age":  25,
		},
	}

	msg := NewUpdateMessage(updateMsg)

	assert.NotNil(t, msg)
	assert.Equal(t, now, msg.EventTime)
	assert.Equal(t, "users", msg.TableName)
	assert.Equal(t, "public", msg.TableNamespace)
	assert.Equal(t, updateMsg.OldDecoded, msg.OldData)
	assert.Equal(t, updateMsg.NewDecoded, msg.NewData)
	assert.Equal(t, UpdateMessage, msg.Type)
	assert.False(t, msg.Type.IsInsert())
	assert.True(t, msg.Type.IsUpdate())
	assert.False(t, msg.Type.IsDelete())
}

func TestNewUpdateMessageWithNilOldData(t *testing.T) {
	now := time.Now()
	updateMsg := &format.Update{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded:     nil,
		NewDecoded: map[string]any{
			"id":   1,
			"name": "new",
		},
	}

	msg := NewUpdateMessage(updateMsg)

	assert.NotNil(t, msg)
	assert.Equal(t, now, msg.EventTime)
	assert.Equal(t, "users", msg.TableName)
	assert.Nil(t, msg.OldData)
	assert.Equal(t, updateMsg.NewDecoded, msg.NewData)
	assert.Equal(t, UpdateMessage, msg.Type)
}

func TestNewDeleteMessage(t *testing.T) {
	now := time.Now()
	deleteMsg := &format.Delete{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded: map[string]any{
			"id":   1,
			"name": "test",
			"age":  25,
		},
	}

	msg := NewDeleteMessage(deleteMsg)

	assert.NotNil(t, msg)
	assert.Equal(t, now, msg.EventTime)
	assert.Equal(t, "users", msg.TableName)
	assert.Equal(t, "public", msg.TableNamespace)
	assert.Equal(t, deleteMsg.OldDecoded, msg.OldData)
	assert.Nil(t, msg.NewData)
	assert.Equal(t, DeleteMessage, msg.Type)
	assert.False(t, msg.Type.IsInsert())
	assert.False(t, msg.Type.IsUpdate())
	assert.True(t, msg.Type.IsDelete())
}

func TestNewDeleteMessageWithEmptyData(t *testing.T) {
	now := time.Now()
	deleteMsg := &format.Delete{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded:     map[string]any{},
	}

	msg := NewDeleteMessage(deleteMsg)

	assert.NotNil(t, msg)
	assert.Equal(t, now, msg.EventTime)
	assert.Equal(t, "users", msg.TableName)
	assert.Equal(t, "public", msg.TableNamespace)
	assert.NotNil(t, msg.OldData)
	assert.Empty(t, msg.OldData)
	assert.Nil(t, msg.NewData)
	assert.Equal(t, DeleteMessage, msg.Type)
}

func TestMessageTypeIsInsert(t *testing.T) {
	assert.True(t, InsertMessage.IsInsert())
	assert.False(t, UpdateMessage.IsInsert())
	assert.False(t, DeleteMessage.IsInsert())
	assert.False(t, MessageType("UNKNOWN").IsInsert())
}

func TestMessageTypeIsUpdate(t *testing.T) {
	assert.False(t, InsertMessage.IsUpdate())
	assert.True(t, UpdateMessage.IsUpdate())
	assert.False(t, DeleteMessage.IsUpdate())
	assert.False(t, MessageType("UNKNOWN").IsUpdate())
}

func TestMessageTypeIsDelete(t *testing.T) {
	assert.False(t, InsertMessage.IsDelete())
	assert.False(t, UpdateMessage.IsDelete())
	assert.True(t, DeleteMessage.IsDelete())
	assert.False(t, MessageType("UNKNOWN").IsDelete())
}

func TestMessageTypeConstants(t *testing.T) {
	assert.Equal(t, MessageType("INSERT"), InsertMessage)
	assert.Equal(t, MessageType("UPDATE"), UpdateMessage)
	assert.Equal(t, MessageType("DELETE"), DeleteMessage)
}

func TestMessageTypeString(t *testing.T) {
	assert.Equal(t, "INSERT", string(InsertMessage))
	assert.Equal(t, "UPDATE", string(UpdateMessage))
	assert.Equal(t, "DELETE", string(DeleteMessage))
}

func TestNewInsertMessagePreservesData(t *testing.T) {
	now := time.Now()
	originalData := map[string]any{
		"id":       42,
		"username": "john_doe",
		"email":    "john@example.com",
		"active":   true,
		"score":    95.5,
	}

	insertMsg := &format.Insert{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		Decoded:        originalData,
	}

	msg := NewInsertMessage(insertMsg)

	assert.Equal(t, originalData, msg.NewData)
	assert.Equal(t, 42, msg.NewData["id"])
	assert.Equal(t, "john_doe", msg.NewData["username"])
	assert.Equal(t, "john@example.com", msg.NewData["email"])
	assert.Equal(t, true, msg.NewData["active"])
	assert.Equal(t, 95.5, msg.NewData["score"])
}

func TestNewUpdateMessagePreservesBothOldAndNewData(t *testing.T) {
	now := time.Now()
	oldData := map[string]any{
		"id":   1,
		"name": "old_name",
	}
	newData := map[string]any{
		"id":   1,
		"name": "new_name",
	}

	updateMsg := &format.Update{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded:     oldData,
		NewDecoded:     newData,
	}

	msg := NewUpdateMessage(updateMsg)

	assert.Equal(t, oldData, msg.OldData)
	assert.Equal(t, newData, msg.NewData)
	assert.Equal(t, "old_name", msg.OldData["name"])
	assert.Equal(t, "new_name", msg.NewData["name"])
}

func TestNewDeleteMessagePreservesOldData(t *testing.T) {
	now := time.Now()
	oldData := map[string]any{
		"id":       100,
		"username": "deleted_user",
		"deleted":  true,
	}

	deleteMsg := &format.Delete{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded:     oldData,
	}

	msg := NewDeleteMessage(deleteMsg)

	assert.Equal(t, oldData, msg.OldData)
	assert.Nil(t, msg.NewData)
	assert.Equal(t, 100, msg.OldData["id"])
	assert.Equal(t, "deleted_user", msg.OldData["username"])
	assert.Equal(t, true, msg.OldData["deleted"])
}

func TestMessageTypeComparison(t *testing.T) {
	// Test that MessageType can be compared directly
	assert.Equal(t, InsertMessage, MessageType("INSERT"))
	assert.Equal(t, UpdateMessage, MessageType("UPDATE"))
	assert.Equal(t, DeleteMessage, MessageType("DELETE"))
	assert.NotEqual(t, InsertMessage, UpdateMessage)
	assert.NotEqual(t, UpdateMessage, DeleteMessage)
	assert.NotEqual(t, InsertMessage, DeleteMessage)
}

func TestNewInsertMessageWithNilDecoded(t *testing.T) {
	now := time.Now()
	insertMsg := &format.Insert{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		Decoded:        nil,
	}

	msg := NewInsertMessage(insertMsg)

	assert.NotNil(t, msg)
	assert.Equal(t, now, msg.EventTime)
	assert.Equal(t, "users", msg.TableName)
	assert.Nil(t, msg.OldData)
	assert.Nil(t, msg.NewData)
	assert.Equal(t, InsertMessage, msg.Type)
}

func TestNewUpdateMessageWithNilNewDecoded(t *testing.T) {
	now := time.Now()
	updateMsg := &format.Update{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded: map[string]any{
			"id": 1,
		},
		NewDecoded: nil,
	}

	msg := NewUpdateMessage(updateMsg)

	assert.NotNil(t, msg)
	assert.Equal(t, now, msg.EventTime)
	assert.NotNil(t, msg.OldData)
	assert.Nil(t, msg.NewData)
	assert.Equal(t, UpdateMessage, msg.Type)
}

func TestNewDeleteMessageWithNilOldDecoded(t *testing.T) {
	now := time.Now()
	deleteMsg := &format.Delete{
		MessageTime:    now,
		TableName:      "users",
		TableNamespace: "public",
		OldDecoded:     nil,
	}

	msg := NewDeleteMessage(deleteMsg)

	assert.NotNil(t, msg)
	assert.Equal(t, now, msg.EventTime)
	assert.Equal(t, "users", msg.TableName)
	assert.Nil(t, msg.OldData)
	assert.Nil(t, msg.NewData)
	assert.Equal(t, DeleteMessage, msg.Type)
}
