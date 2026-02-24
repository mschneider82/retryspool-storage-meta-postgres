package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	metastorage "schneider.vip/retryspool/storage/meta"
)

// Backend implements PostgreSQL metadata storage
type Backend struct {
	db        *sql.DB
	tableName string
}

// createTable creates the messages table if it doesn't exist
func (b *Backend) createTable() error {
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id VARCHAR(255) PRIMARY KEY,
		state INTEGER NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL,
		next_retry TIMESTAMP WITH TIME ZONE NOT NULL,
		created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		last_error TEXT,
		size BIGINT NOT NULL DEFAULT 0,
		priority INTEGER NOT NULL DEFAULT 5,
		headers JSONB,
		retry_policy_name TEXT
	);
	
	-- Create indexes for efficient querying
	CREATE INDEX IF NOT EXISTS idx_%s_state ON %s(state);
	CREATE INDEX IF NOT EXISTS idx_%s_next_retry ON %s(next_retry) WHERE state = %d;
	CREATE INDEX IF NOT EXISTS idx_%s_priority ON %s(priority, created);
	CREATE INDEX IF NOT EXISTS idx_%s_created ON %s(created);
	`,
		b.tableName,
		b.tableName, b.tableName,
		b.tableName, b.tableName, int(metastorage.StateDeferred),
		b.tableName, b.tableName,
		b.tableName, b.tableName,
	)

	_, err := b.db.Exec(query)
	return err
}

// StoreMeta stores message metadata with transaction support
func (b *Backend) StoreMeta(ctx context.Context, messageID string, metadata metastorage.MessageMetadata) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	headersJSON, err := json.Marshal(metadata.Headers)
	if err != nil {
		return fmt.Errorf("failed to marshal headers: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, state, attempts, max_attempts, next_retry, created, updated, last_error, size, priority, headers, retry_policy_name)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, b.tableName)

	_, err = tx.ExecContext(ctx, query,
		messageID,
		int(metadata.State),
		metadata.Attempts,
		metadata.MaxAttempts,
		metadata.NextRetry,
		metadata.Created,
		metadata.Updated,
		metadata.LastError,
		metadata.Size,
		metadata.Priority,
		headersJSON,
		metadata.RetryPolicyName,
	)

	if err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetMeta retrieves message metadata
func (b *Backend) GetMeta(ctx context.Context, messageID string) (metastorage.MessageMetadata, error) {
	var metadata metastorage.MessageMetadata
	var state int
	var headersJSON []byte

	query := fmt.Sprintf(`
		SELECT state, attempts, max_attempts, next_retry, created, updated, last_error, size, priority, headers, retry_policy_name
		FROM %s WHERE id = $1
	`, b.tableName)

	row := b.db.QueryRowContext(ctx, query, messageID)
	err := row.Scan(
		&state,
		&metadata.Attempts,
		&metadata.MaxAttempts,
		&metadata.NextRetry,
		&metadata.Created,
		&metadata.Updated,
		&metadata.LastError,
		&metadata.Size,
		&metadata.Priority,
		&headersJSON,
		&metadata.RetryPolicyName,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return metadata, metastorage.ErrMessageNotFound
		}
		return metadata, fmt.Errorf("failed to get metadata: %w", err)
	}

	metadata.ID = messageID
	metadata.State = metastorage.QueueState(state)

	if err := json.Unmarshal(headersJSON, &metadata.Headers); err != nil {
		return metadata, fmt.Errorf("failed to unmarshal headers: %w", err)
	}

	return metadata, nil
}

// UpdateMeta updates message metadata with transaction support
func (b *Backend) UpdateMeta(ctx context.Context, messageID string, metadata metastorage.MessageMetadata) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	headersJSON, err := json.Marshal(metadata.Headers)
	if err != nil {
		return fmt.Errorf("failed to marshal headers: %w", err)
	}

	query := fmt.Sprintf(`
		UPDATE %s SET 
			state = $2, 
			attempts = $3, 
			max_attempts = $4, 
			next_retry = $5, 
			updated = $6, 
			last_error = $7, 
			size = $8, 
			priority = $9, 
			headers = $10,
			retry_policy_name = $11
		WHERE id = $1
	`, b.tableName)

	result, err := tx.ExecContext(ctx, query,
		messageID,
		int(metadata.State),
		metadata.Attempts,
		metadata.MaxAttempts,
		metadata.NextRetry,
		metadata.Updated,
		metadata.LastError,
		metadata.Size,
		metadata.Priority,
		headersJSON,
		metadata.RetryPolicyName,
	)

	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return metastorage.ErrMessageNotFound
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// DeleteMeta removes message metadata with transaction support
func (b *Backend) DeleteMeta(ctx context.Context, messageID string) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	query := fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, b.tableName)

	result, err := tx.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return metastorage.ErrMessageNotFound
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// ListMessages lists messages with pagination and filtering
func (b *Backend) ListMessages(ctx context.Context, state metastorage.QueueState, options metastorage.MessageListOptions) (metastorage.MessageListResult, error) {
	var result metastorage.MessageListResult

	// Build query based on options
	baseQuery := fmt.Sprintf(`SELECT id FROM %s WHERE state = $1`, b.tableName)
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE state = $1`, b.tableName)
	args := []interface{}{int(state)}
	argIndex := 2

	// Add time filtering
	if !options.Since.IsZero() {
		timeFilter := fmt.Sprintf(` AND updated >= $%d`, argIndex)
		baseQuery += timeFilter
		countQuery += timeFilter
		args = append(args, options.Since)
		argIndex++
	}

	// Add sorting
	var orderBy string
	switch options.SortBy {
	case "created":
		orderBy = "created"
	case "updated":
		orderBy = "updated"
	case "priority":
		orderBy = "priority"
	case "attempts":
		orderBy = "attempts"
	default:
		orderBy = "created"
	}

	if options.SortOrder == "desc" {
		orderBy += " DESC"
	} else {
		orderBy += " ASC"
	}

	baseQuery += fmt.Sprintf(` ORDER BY %s`, orderBy)

	// Save args for count query (before adding LIMIT/OFFSET)
	countArgs := make([]interface{}, len(args))
	copy(countArgs, args)

	// Add pagination
	if options.Limit > 0 {
		baseQuery += fmt.Sprintf(` LIMIT $%d`, argIndex)
		args = append(args, options.Limit)
		argIndex++
	}

	if options.Offset > 0 {
		baseQuery += fmt.Sprintf(` OFFSET $%d`, argIndex)
		args = append(args, options.Offset)
		argIndex++
	}

	// Get total count
	var total int
	err := b.db.QueryRowContext(ctx, countQuery, countArgs...).Scan(&total)
	if err != nil {
		return result, fmt.Errorf("failed to get total count: %w", err)
	}

	result.Total = total

	// Get message IDs
	rows, err := b.db.QueryContext(ctx, baseQuery, args...)
	if err != nil {
		return result, fmt.Errorf("failed to list messages: %w", err)
	}
	defer rows.Close()

	var messageIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return result, fmt.Errorf("failed to scan message ID: %w", err)
		}
		messageIDs = append(messageIDs, id)
	}

	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("error iterating rows: %w", err)
	}

	result.MessageIDs = messageIDs
	result.HasMore = (options.Offset + len(messageIDs)) < total

	return result, nil
}

// NewMessageIterator creates an iterator for messages in a specific state
func (b *Backend) NewMessageIterator(ctx context.Context, state metastorage.QueueState, batchSize int) (metastorage.MessageIterator, error) {
	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}

	return &postgresIterator{
		backend:   b,
		state:     state,
		batchSize: batchSize,
		ctx:       ctx,
		offset:    0,
	}, nil
}

// postgresIterator implements MessageIterator for PostgreSQL backend
type postgresIterator struct {
	backend   *Backend
	state     metastorage.QueueState
	batchSize int
	ctx       context.Context

	// Current state
	rows     *sql.Rows
	current  []metastorage.MessageMetadata
	index    int
	offset   int
	finished bool
	closed   bool
}

// Next returns the next message metadata
func (it *postgresIterator) Next(ctx context.Context) (metastorage.MessageMetadata, bool, error) {
	if it.closed {
		return metastorage.MessageMetadata{}, false, fmt.Errorf("iterator is closed")
	}

	// Initialize or load next batch if needed
	if it.current == nil || (it.index >= len(it.current) && !it.finished) {
		if err := it.loadBatch(ctx); err != nil {
			return metastorage.MessageMetadata{}, false, err
		}
	}

	// Check if we're at the end
	if it.index >= len(it.current) {
		return metastorage.MessageMetadata{}, false, nil
	}

	// Return current message and advance
	metadata := it.current[it.index]
	it.index++

	// Check if there are more messages
	hasMore := it.index < len(it.current) || !it.finished

	return metadata, hasMore, nil
}

// loadBatch loads the next batch of messages from the database
func (it *postgresIterator) loadBatch(ctx context.Context) error {
	if it.finished {
		return nil
	}

	// Build query for full metadata
	query := fmt.Sprintf(`
		SELECT id, state, attempts, max_attempts, next_retry, created, updated, 
			   last_error, size, priority, headers, retry_policy_name
		FROM %s 
		WHERE state = $1 
		ORDER BY created ASC 
		LIMIT $2 OFFSET $3
	`, it.backend.tableName)

	rows, err := it.backend.db.QueryContext(ctx, query, int(it.state), it.batchSize, it.offset)
	if err != nil {
		return fmt.Errorf("failed to query messages: %w", err)
	}
	defer rows.Close()

	// Load batch into memory
	var batch []metastorage.MessageMetadata
	for rows.Next() {
		var metadata metastorage.MessageMetadata
		var headersJSON []byte

		err := rows.Scan(
			&metadata.ID,
			&metadata.State,
			&metadata.Attempts,
			&metadata.MaxAttempts,
			&metadata.NextRetry,
			&metadata.Created,
			&metadata.Updated,
			&metadata.LastError,
			&metadata.Size,
			&metadata.Priority,
			&headersJSON,
			&metadata.RetryPolicyName,
		)
		if err != nil {
			return fmt.Errorf("failed to scan message: %w", err)
		}

		// Unmarshal headers
		if len(headersJSON) > 0 {
			if err := json.Unmarshal(headersJSON, &metadata.Headers); err != nil {
				return fmt.Errorf("failed to unmarshal headers: %w", err)
			}
		}

		batch = append(batch, metadata)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	// Update iterator state
	it.current = batch
	it.index = 0
	it.offset += len(batch)

	// Check if we're finished (batch smaller than requested)
	if len(batch) < it.batchSize {
		it.finished = true
	}

	return nil
}

// Close closes the iterator and any open resources
func (it *postgresIterator) Close() error {
	it.closed = true
	if it.rows != nil {
		it.rows.Close()
		it.rows = nil
	}
	return nil
}

// MoveToState moves a message from one queue state to another atomically with transaction support
func (b *Backend) MoveToState(ctx context.Context, messageID string, fromState, toState metastorage.QueueState) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Atomic update: only move if in expected fromState
	query := fmt.Sprintf(`
		UPDATE %s SET state = $3, updated = $4 WHERE id = $1 AND state = $2
	`, b.tableName)

	result, err := tx.ExecContext(ctx, query, messageID, int(fromState), int(toState), time.Now())
	if err != nil {
		return fmt.Errorf("failed to move message to state: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// Check if message exists but is in wrong state
		var currentState int
		checkQuery := fmt.Sprintf(`SELECT state FROM %s WHERE id = $1`, b.tableName)
		err := tx.QueryRowContext(ctx, checkQuery, messageID).Scan(&currentState)
		if err == sql.ErrNoRows {
			return metastorage.ErrMessageNotFound
		} else if err != nil {
			return fmt.Errorf("failed to check current state: %w", err)
		}
		return metastorage.ErrStateConflict
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Close closes the database connection
func (b *Backend) Close() error {
	return b.db.Close()
}
