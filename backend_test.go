package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	metastorage "schneider.vip/retryspool/storage/meta"
	_ "github.com/lib/pq"
)

func getTestDSN() string {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:password@localhost/retryspool_test?sslmode=disable"
	}
	return dsn
}

func setupTestDB(t *testing.T) *Backend {
	dsn := getTestDSN()
	
	// Try to create a test database
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	factory := NewFactory(dsn).WithTableName("test_messages")
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	return backend.(*Backend)
}

func cleanupTestDB(t *testing.T, backend *Backend) {
	// Clean up test data
	_, err := backend.db.Exec("DROP TABLE IF EXISTS test_messages")
	if err != nil {
		t.Logf("Failed to cleanup test table: %v", err)
	}
	backend.Close()
}

func TestBackend_StoreMeta(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	messageID := "test-message-1"
	
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		Attempts:    0,
		MaxAttempts: 5,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		LastError:   "",
		Size:        1024,
		Priority:    5,
		Headers: map[string]string{
			"to":   "test@example.com",
			"from": "sender@example.com",
		},
	}

	err := backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}

	// Verify the metadata was stored
	retrieved, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	if retrieved.ID != metadata.ID {
		t.Errorf("Expected ID %s, got %s", metadata.ID, retrieved.ID)
	}
	if retrieved.State != metadata.State {
		t.Errorf("Expected state %d, got %d", metadata.State, retrieved.State)
	}
	if retrieved.Headers["to"] != metadata.Headers["to"] {
		t.Errorf("Expected header to=%s, got %s", metadata.Headers["to"], retrieved.Headers["to"])
	}
}

func TestBackend_UpdateMeta(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	messageID := "test-message-2"
	
	// Store initial metadata
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		Attempts:    0,
		MaxAttempts: 5,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		Size:        1024,
		Priority:    5,
		Headers:     map[string]string{"test": "value"},
	}

	err := backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}

	// Update metadata
	metadata.State = metastorage.StateActive
	metadata.Attempts = 1
	metadata.LastError = "test error"
	metadata.Updated = time.Now()

	err = backend.UpdateMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to update metadata: %v", err)
	}

	// Verify the update
	retrieved, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get updated metadata: %v", err)
	}

	if retrieved.State != metastorage.StateActive {
		t.Errorf("Expected state Active, got %d", retrieved.State)
	}
	if retrieved.Attempts != 1 {
		t.Errorf("Expected attempts 1, got %d", retrieved.Attempts)
	}
	if retrieved.LastError != "test error" {
		t.Errorf("Expected last error 'test error', got %s", retrieved.LastError)
	}
}

func TestBackend_DeleteMeta(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	messageID := "test-message-3"
	
	// Store metadata
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		MaxAttempts: 5,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		Size:        1024,
		Priority:    5,
		Headers:     map[string]string{},
	}

	err := backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}

	// Delete metadata
	err = backend.DeleteMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to delete metadata: %v", err)
	}

	// Verify deletion
	_, err = backend.GetMeta(ctx, messageID)
	if err != metastorage.ErrMessageNotFound {
		t.Errorf("Expected ErrMessageNotFound, got %v", err)
	}

	// Try to delete non-existent message
	err = backend.DeleteMeta(ctx, "non-existent")
	if err != metastorage.ErrMessageNotFound {
		t.Errorf("Expected ErrMessageNotFound for non-existent message, got %v", err)
	}
}

func TestBackend_ListMessages(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	
	// Store multiple messages
	for i := 0; i < 5; i++ {
		messageID := fmt.Sprintf("test-message-%d", i)
		metadata := metastorage.MessageMetadata{
			ID:          messageID,
			State:       metastorage.StateIncoming,
			MaxAttempts: 5,
			NextRetry:   time.Now().Add(time.Hour),
			Created:     time.Now().Add(time.Duration(i) * time.Minute),
			Updated:     time.Now(),
			Size:        1024,
			Priority:    i + 1,
			Headers:     map[string]string{},
		}

		err := backend.StoreMeta(ctx, messageID, metadata)
		if err != nil {
			t.Fatalf("Failed to store metadata %d: %v", i, err)
		}
	}

	// List all messages
	result, err := backend.ListMessages(ctx, metastorage.StateIncoming, metastorage.MessageListOptions{})
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}

	if result.Total != 5 {
		t.Errorf("Expected total 5, got %d", result.Total)
	}
	if len(result.MessageIDs) != 5 {
		t.Errorf("Expected 5 message IDs, got %d", len(result.MessageIDs))
	}

	// Test pagination
	result, err = backend.ListMessages(ctx, metastorage.StateIncoming, metastorage.MessageListOptions{
		Limit:  2,
		Offset: 0,
	})
	if err != nil {
		t.Fatalf("Failed to list messages with pagination: %v", err)
	}

	if len(result.MessageIDs) != 2 {
		t.Errorf("Expected 2 message IDs, got %d", len(result.MessageIDs))
	}
	if !result.HasMore {
		t.Error("Expected HasMore to be true")
	}

	// Test sorting
	result, err = backend.ListMessages(ctx, metastorage.StateIncoming, metastorage.MessageListOptions{
		SortBy:    "priority",
		SortOrder: "desc",
	})
	if err != nil {
		t.Fatalf("Failed to list messages with sorting: %v", err)
	}

	if len(result.MessageIDs) != 5 {
		t.Errorf("Expected 5 message IDs, got %d", len(result.MessageIDs))
	}
}

func TestBackend_MoveToState(t *testing.T) {
	backend := setupTestDB(t)
	defer cleanupTestDB(t, backend)

	ctx := context.Background()
	messageID := "test-message-4"
	
	// Store metadata
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		MaxAttempts: 5,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		Size:        1024,
		Priority:    5,
		Headers:     map[string]string{},
	}

	err := backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}

	// Move to active state
	err = backend.MoveToState(ctx, messageID, metastorage.StateActive)
	if err != nil {
		t.Fatalf("Failed to move to state: %v", err)
	}

	// Verify state change
	retrieved, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	if retrieved.State != metastorage.StateActive {
		t.Errorf("Expected state Active, got %d", retrieved.State)
	}

	// Try to move non-existent message
	err = backend.MoveToState(ctx, "non-existent", metastorage.StateActive)
	if err != metastorage.ErrMessageNotFound {
		t.Errorf("Expected ErrMessageNotFound for non-existent message, got %v", err)
	}
}

func TestFactory_Create(t *testing.T) {
	dsn := getTestDSN()
	
	factory := NewFactory(dsn)
	backend, err := factory.Create()
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	defer backend.Close()

	if factory.Name() != "postgres" {
		t.Errorf("Expected factory name 'postgres', got %s", factory.Name())
	}
}