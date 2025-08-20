# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

## Project Overview

This is a distributed job processing system written in Go that implements a PostgreSQL-backed job queue with worker processing. The system supports multi-tenant job execution with round-robin fairness across organizations.

## Common Commands

### Build and Run
```bash
# Build the application
go build -o bin/job_worker *.go

# Initialize database schema
go run *.go -mode=init -dsn="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

# Run a worker instance
go run *.go -mode=worker -dsn="postgres://..." -prefetch=1000 -inflight=400

# Create a task with jobs for testing
go run *.go -mode=create-task -dsn="postgres://..." -org=orgA -jobs=250
```

### Dependencies
```bash
# Install dependencies
go mod tidy

# Update dependencies
go mod download
```

## Architecture

### Core Components

1. **Job Queue System** (`job.go`): Handles job fetching, claiming, and status updates with atomic operations
2. **Worker Engine** (`worker.go`): Implements prefetched round-robin scheduling with organization-level fairness
3. **Task Management** (`task.go`): Manages task lifecycle, progress tracking, and monitoring
4. **Database Schema** (`schema.go`): PostgreSQL schema with tasks and jobs tables

### Key Design Patterns

- **Atomic Job Claiming**: Uses `UPDATE ... WHERE status='pending' RETURNING ...` to prevent race conditions
- **Prefetched Candidates**: Workers maintain in-memory candidate sets to reduce database load
- **Round-Robin Fairness**: Per-organization job distribution using weighted queues
- **Concurrency Control**: Total system throughput controlled by inflight job limit
- **Graceful Degradation**: Jobs automatically retry with exponential backoff up to `max_attempts`

### Database Tables

- `tasks`: Top-level work units with progress tracking (`current_step` JSONB field)
- `jobs`: Individual executable units linked to tasks, with JSONB `input`/`result` fields

### Worker Configuration

Worker tuning parameters:
- `-prefetch=N`: Target candidate pool size (default: 1000)
- `-inflight=N`: Max concurrent job executions (default: 400)
- `-stats_interval=5s`: Enable periodic stats logging

## Dependencies

- `github.com/jackc/pgx/v5`: PostgreSQL driver with connection pooling
- Standard library: `context`, `sync`, `json`, `time`
