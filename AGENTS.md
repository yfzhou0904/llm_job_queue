# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

## Project Overview

This is a distributed job processing system written in Go that implements a PostgreSQL-backed job queue with rate-limited worker processing. The system supports multi-tenant job execution with per-provider rate limiting and round-robin fairness across organizations.

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
go run *.go -mode=create-task -dsn="postgres://..." -org=orgA -jobs=250 -provider=openai
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
2. **Worker Engine** (`worker.go`): Implements prefetched round-robin scheduling with provider-specific rate limiting
3. **Task Management** (`task.go`): Manages task lifecycle, progress tracking, and monitoring
4. **Database Schema** (`schema.go`): PostgreSQL schema with tasks and jobs tables

### Key Design Patterns

- **Atomic Job Claiming**: Uses `UPDATE ... WHERE status='pending' RETURNING ...` to prevent race conditions
- **Prefetched Candidates**: Workers maintain in-memory candidate sets to reduce database load
- **Round-Robin Fairness**: Per-organization job distribution using weighted queues
- **Provider Rate Limiting**: Per-provider RPS caps using `golang.org/x/time/rate`
- **Graceful Degradation**: Jobs automatically retry with exponential backoff up to `max_attempts`

### Database Tables

- `tasks`: Top-level work units with progress tracking (`current_step` JSONB field)
- `jobs`: Individual executable units linked to tasks, with JSONB `input`/`result` fields

### Worker Configuration

Default provider RPS limits (configurable via flags):
- OpenAI: 120 RPS (`-rps.openai=N`)
- Anthropic: 80 RPS (`-rps.anthropic=N`)
- Gemini: 60 RPS (`-rps.gemini=N`)

Worker tuning parameters:
- `-prefetch=N`: Target candidate pool size (default: 1000)
- `-inflight=N`: Max concurrent job executions (default: 400)
- `-stats_interval=5s`: Enable periodic stats logging

## Dependencies

- `github.com/jackc/pgx/v5`: PostgreSQL driver with connection pooling
- `golang.org/x/time/rate`: Token bucket rate limiting
- Standard library: `context`, `sync`, `json`, `time`
