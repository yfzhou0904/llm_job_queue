package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func initSchema(ctx context.Context, db *pgxpool.Pool) error {
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS pgcrypto;`,
		// tasks table (mirrors your existing shape, simplified)
		`CREATE TABLE IF NOT EXISTS tasks (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			org_id TEXT NOT NULL,
			user_id TEXT,
			type TEXT NOT NULL,
			input JSONB NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending', -- pending|running|completed|failed
			result JSONB,
			current_step JSONB,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			finished_at TIMESTAMPTZ
		);`,
		// Backfill-safe add for existing DBs
		`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS current_step JSONB;`,
		// jobs table (general queue)
		`CREATE TABLE IF NOT EXISTS jobs (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			org_id TEXT NOT NULL,
			task_id uuid REFERENCES tasks(id),
			type TEXT NOT NULL,
			input JSONB NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',  -- pending|processing|succeeded|failed
			result JSONB,
			errors TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
			attempts INT NOT NULL DEFAULT 0,
			max_attempts INT NOT NULL DEFAULT 3,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);`,
		`CREATE INDEX IF NOT EXISTS jobs_status_prio_created_idx ON jobs(status, created_at);`,
		`CREATE INDEX IF NOT EXISTS jobs_org_status_idx ON jobs(org_id, status);`,
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, s := range stmts {
		if _, err := tx.Exec(ctx, s); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
