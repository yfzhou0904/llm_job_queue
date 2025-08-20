package main

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type JobMeta struct {
	ID   string
	Org  string
	Type string
}

type ClaimedJob struct {
	ID         string
	Org        string
	Type       string
	Input      json.RawMessage
	Attempts   int
	MaxAttempt int
}

// fetchCandidates pulls light candidates (no JSONB) for pending jobs.
// NOTE: We do NOT lease here; claim happens at dispatch via UPDATE ... RETURNING.
func fetchCandidates(ctx context.Context, db *pgxpool.Pool, n int) ([]JobMeta, error) {
	rows, err := db.Query(ctx, `
		SELECT id::text, org_id, type
		FROM jobs
		WHERE status='pending'
		ORDER BY created_at
		LIMIT $1
	`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []JobMeta
	for rows.Next() {
		var m JobMeta
		if err := rows.Scan(&m.ID, &m.Org, &m.Type); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func claimJob(ctx context.Context, db *pgxpool.Pool, id string) (ClaimedJob, bool, error) {
	var cj ClaimedJob
	err := db.QueryRow(ctx, `
		UPDATE jobs
		SET status='processing', updated_at=now()
		WHERE id=$1 AND status='pending'
		RETURNING id::text, org_id, type, input, attempts, max_attempts
	`, id).Scan(&cj.ID, &cj.Org, &cj.Type, &cj.Input, &cj.Attempts, &cj.MaxAttempt)
	if err != nil {
		// Not found or not pending → no rows
		if strings.Contains(err.Error(), "no rows") {
			return ClaimedJob{}, false, nil
		}
		return ClaimedJob{}, false, err
	}
	return cj, true, nil
}

func appendErrorOrRetry(ctx context.Context, db *pgxpool.Pool, id string, msg string) error {
	_, err := db.Exec(ctx, `
		UPDATE jobs
		SET errors = array_append(errors, $2),
			attempts = attempts + 1,
			status = CASE WHEN attempts + 1 >= max_attempts THEN 'failed' ELSE 'pending' END,
			updated_at = now()
		WHERE id = $1
	`, id, msg)
	return err
}

func markSucceeded(ctx context.Context, db *pgxpool.Pool, id string, resultJSON string) error {
	_, err := db.Exec(ctx, `
		UPDATE jobs
		SET status='succeeded', result=$2::jsonb, updated_at=now()
		WHERE id=$1
	`, id, resultJSON)
	return err
}