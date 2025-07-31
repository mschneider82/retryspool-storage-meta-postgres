# PostgreSQL Meta Storage for RetrySpool

PostgreSQL implementation for RetrySpool message metadata storage.

## Overview

This module provides a PostgreSQL backend for storing message metadata in RetrySpool queues. It implements the `metastorage.Backend` interface with full transaction support for data consistency.

## Features

- **Transaction Support**: All write operations use explicit transactions with automatic rollback on errors
- **JSONB Headers**: Message headers stored as JSONB for efficient querying
- **Optimized Indexes**: Strategic indexes for performance on common query patterns
- **Connection Pooling**: Configurable connection limits for optimal resource usage
- **Multi-Worker Optimized**: Thread-safe operations designed for concurrent worker environments
- **Efficient Batching**: SQL LIMIT/OFFSET for memory-efficient message iteration
- **Custom Table Names**: Support for custom table names in multi-tenant scenarios
- **Production Ready**: Enterprise-grade reliability for high-throughput workloads

## Installation

```bash
go get schneider.vip/retryspool/storage/meta/postgres
```

## Usage

### Basic Usage

```go
package main

import (
    "context"
    
    "schneider.vip/retryspool"
    postgres "schneider.vip/retryspool/storage/meta/postgres"
)

func main() {
    // Create PostgreSQL meta storage factory
    metaFactory := postgres.NewFactory("postgres://user:password@localhost/retryspool?sslmode=disable")
    
    // Create queue with PostgreSQL meta storage
    queue := retryspool.New(
        retryspool.WithMetaStorage(metaFactory),
        // ... other options
    )
    defer queue.Close()
}
```

### Advanced Configuration

```go
// Custom table name and connection limits
metaFactory := postgres.NewFactory(dsn).
    WithTableName("custom_messages").
    WithConnectionLimits(50, 10) // max 50 open, 10 idle connections

queue := retryspool.New(
    retryspool.WithMetaStorage(metaFactory),
)
```

## Database Schema

The module automatically creates the following table structure:

```sql
CREATE TABLE retryspool_messages (
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
    headers JSONB
);

-- Indexes for efficient querying
CREATE INDEX idx_retryspool_messages_state ON retryspool_messages(state);
CREATE INDEX idx_retryspool_messages_next_retry ON retryspool_messages(next_retry) WHERE state = 2; -- StateDeferred
CREATE INDEX idx_retryspool_messages_priority ON retryspool_messages(priority, created);
CREATE INDEX idx_retryspool_messages_created ON retryspool_messages(created);
```

## Configuration Options

### Factory Options

- `WithTableName(name string)`: Set custom table name (default: "retryspool_messages")
- `WithConnectionLimits(maxOpen, maxIdle int)`: Configure connection pool (default: 25, 5)

### Environment Variables

For testing, you can set:
- `POSTGRES_TEST_DSN`: PostgreSQL connection string for tests

## Transaction Guarantees

All write operations are wrapped in transactions:

- **StoreMeta**: Atomic metadata insertion
- **UpdateMeta**: Atomic metadata updates with optimistic locking
- **DeleteMeta**: Atomic metadata deletion
- **MoveToState**: Atomic state transitions

Read operations (`GetMeta`, `ListMessages`) don't use transactions for better performance.

## Performance Considerations

- **Indexes**: Optimized for common query patterns (state, priority, timestamps)
- **JSONB**: Headers stored as JSONB for efficient JSON operations
- **Connection Pooling**: Configurable limits to balance performance and resource usage
- **Prepared Statements**: All queries use parameterized statements

## Error Handling

The module returns specific errors:
- `metastorage.ErrMessageNotFound`: When message doesn't exist
- Transaction errors: Wrapped with context for debugging

## Testing

```bash
# Set PostgreSQL connection for testing
export POSTGRES_TEST_DSN="postgres://user:password@localhost/test_db?sslmode=disable"

# Run tests
go test -v
```

Tests will skip if PostgreSQL is not available.

## Dependencies

- `github.com/lib/pq`: PostgreSQL driver
- `schneider.vip/retryspool/storage/meta`: Meta storage interface

## License

Same as RetrySpool main project.