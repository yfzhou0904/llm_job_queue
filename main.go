// main.go
//
// Usage examples:
//
// 1) Init schema:
//    go run main.go -mode=init -dsn="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
//
// 2) Run worker (single machine):
//    go run main.go -mode=worker -dsn="postgres://..." -prefetch=1000 -inflight=400
//
// 3) Create a task with 250 jobs for org "orgA", provider "openai", then monitor it:
//    go run main.go -mode=create-task -dsn="postgres://..." -org=orgA -jobs=250 -provider=openai
//
// Providers & default RPS caps (override with -rps.<provider>=N):
//   openai=120 rps, anthropic=80 rps, gemini=60 rps
//
// Notes:
// - Single-machine fairness: per-org round-robin over a prefetched candidate set.
// - Claim happens atomically at dispatch time (UPDATE ... WHERE status='pending' RETURNING ...).
// - Job simulation: sleep ~N(30s, 6s^2) clamped to [1s, 120s]; 1% error chance.
// - Job input/result are JSONB; provider is a field in input JSONB.

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	mathrand "math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"
)

var (
	mode      = flag.String("mode", "init", "init | worker | create-task")
	dsn       = flag.String("dsn", "", "Postgres DSN")
	orgFlag   = flag.String("org", "orgA", "Org ID (for create-task)")
	providerF = flag.String("provider", "openai", "Provider name for jobs (for create-task)")
	nJobs     = flag.Int("jobs", 50, "Number of jobs to create (for create-task)")

	// Worker tuning
	prefetchN  = flag.Int("prefetch", 1000, "Target number of prefetched candidate jobs")
	refillTrig = flag.Int("refill_trigger", 300, "Refill when in-memory candidates drop below this")
	inflight   = flag.Int("inflight", 400, "Max concurrent in-flight job executions")

	// Provider RPS overrides, e.g. -rps.openai=200 -rps.anthropic=100
	rpsOpenAI    = flag.Int("rps.openai", 120, "OpenAI RPS cap")
	rpsAnthropic = flag.Int("rps.anthropic", 80, "Anthropic RPS cap")
	rpsGemini    = flag.Int("rps.gemini", 60, "Gemini RPS cap")
)

func main() {
	flag.Parse()
	if *dsn == "" {
		fmt.Println("ERROR: -dsn is required")
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	switch *mode {
	case "init":
		if err := initSchema(ctx, pool); err != nil {
			panic(err)
		}
		fmt.Println("Schema initialized.")
	case "create-task":
		taskID, err := createTaskWithJobs(ctx, pool, *orgFlag, *providerF, *nJobs)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Created task %s with %d jobs for org=%s provider=%s\n", taskID, *nJobs, *orgFlag, *providerF)
		monitorTask(ctx, pool, taskID)
	case "worker":
		if err := runWorker(ctx, pool); err != nil {
			panic(err)
		}
	default:
		fmt.Println("Unknown -mode. Use init | worker | create-task")
	}
}

// ---------------- Schema ----------------

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
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			finished_at TIMESTAMPTZ
		);`,
		// jobs table (general queue)
		`CREATE TABLE IF NOT EXISTS jobs (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			org_id TEXT NOT NULL,
			task_id uuid REFERENCES tasks(id),
			type TEXT NOT NULL,
			input JSONB NOT NULL, -- includes {"provider": "openai" | "anthropic" | ...}
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

// ------------- Task + Job creation -------------

func createTaskWithJobs(ctx context.Context, db *pgxpool.Pool, orgID, provider string, count int) (string, error) {
	// Create task
	taskInput := map[string]any{
		"reason":   "demo load",
		"provider": provider,
	}
	taskInputJSON, _ := json.Marshal(taskInput)

	var taskID string
	err := db.QueryRow(ctx, `
		INSERT INTO tasks (org_id, user_id, type, input, status)
		VALUES ($1, $2, 'sheet.enrich', $3::jsonb, 'running')
		RETURNING id::text
	`, orgID, "user-demo", string(taskInputJSON)).Scan(&taskID)
	if err != nil {
		return "", err
	}

	// Create jobs
	now := time.Now()
	batch := db.Config().ConnConfig.RuntimeParams
	_ = batch // (no-op, just to hint this is a demo; we'll use simple inserts)

	tx, err := db.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	for i := 0; i < count; i++ {
		jobInput := map[string]any{
			"provider": provider,
			"payload": map[string]any{
				"row_index": i,
				"seed":      randHex(8),
			},
		}
		inJSON, _ := json.Marshal(jobInput)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jobs (org_id, task_id, type, input, status, max_attempts, created_at, updated_at)
			VALUES ($1, $2, 'sheet.enrich', $3::jsonb, 'pending', 3, $4, $4)
		`, orgID, taskID, string(inJSON), now); err != nil {
			return "", err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return taskID, nil
}

func monitorTask(ctx context.Context, db *pgxpool.Pool, taskID string) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	start := time.Now()
	for {
		<-ticker.C
		var total, done, failed, processing int
		err := db.QueryRow(ctx, `
			SELECT
			  COUNT(*) FILTER (WHERE task_id = $1) AS total,
			  COUNT(*) FILTER (WHERE task_id = $1 AND status='succeeded') AS done,
			  COUNT(*) FILTER (WHERE task_id = $1 AND status='failed') AS failed,
			  COUNT(*) FILTER (WHERE task_id = $1 AND status='processing') AS processing
			FROM jobs
		`, taskID).Scan(&total, &done, &failed, &processing)
		if err != nil {
			fmt.Println("monitor error:", err)
			continue
		}
		fmt.Printf("[monitor] elapsed=%s total=%d done=%d failed=%d processing=%d pending=%d\n",
			time.Since(start).Truncate(time.Second), total, done, failed, processing, total-done-failed-processing)

		if total > 0 && done+failed == total {
			// mark task completed/failed
			status := "completed"
			if done == 0 {
				status = "failed"
			}
			_, _ = db.Exec(ctx, `
				UPDATE tasks
				SET status=$2, finished_at=now(), updated_at=now(), result=$3::jsonb
				WHERE id=$1
			`, taskID, status, fmt.Sprintf(`{"done":%d,"failed":%d}`, done, failed))
			fmt.Println("Task finished with status:", status)
			return
		}
	}
}

// ---------------- Worker (single machine) ----------------

// JobMeta: light candidate
type JobMeta struct {
	ID  string
	Org string
}

type ClaimedJob struct {
	ID         string
	Org        string
	Type       string
	Input      json.RawMessage
	Attempts   int
	MaxAttempt int
}

func runWorker(ctx context.Context, db *pgxpool.Pool) error {
	// Provider → limiter
	providerRPS := map[string]int{
		"openai":    *rpsOpenAI,
		"anthropic": *rpsAnthropic,
		"gemini":    *rpsGemini,
	}
	providerLimiter := map[string]*rate.Limiter{}
	for p, rps := range providerRPS {
		if rps < 1 {
			rps = 1
		}
		providerLimiter[p] = rate.NewLimiter(rate.Limit(rps), rps) // burst=rps
	}

	// In-flight limiter (concurrency gate)
	inflightCh := make(chan struct{}, *inflight)
	for i := 0; i < *inflight; i++ {
		inflightCh <- struct{}{}
	}

	// In-memory candidate queues (org → FIFO of job IDs)
	queues := map[string][]JobMeta{}
	qMu := sync.Mutex{}

	// Keep a small set of "seen" IDs so the refill loop doesn't re-add
	seen := newStringSet()

	// Refill loop: prefetch candidates into in-memory queues
	go func() {
		for {
			// quick sleep to avoid hammering db
			time.Sleep(1000 * time.Millisecond)

			qMu.Lock()
			totalCandidates := 0
			for _, q := range queues {
				totalCandidates += len(q)
			}
			qMu.Unlock()

			if totalCandidates >= *refillTrig {
				continue
			}
			want := *prefetchN - totalCandidates
			if want < 100 {
				want = 100
			}
			metas, err := fetchCandidates(ctx, db, want)
			if err != nil {
				fmt.Println("refill fetch error:", err)
				continue
			}
			if len(metas) == 0 {
				continue
			}
			qMu.Lock()
			for _, m := range metas {
				if seen.Has(m.ID) {
					continue
				}
				seen.Add(m.ID)
				queues[m.Org] = append(queues[m.Org], m)
			}
			qMu.Unlock()
		}
	}()

	// Round-robin org order
	rrOrder := []string{}
	rrIdx := 0

	// Dispatch loop
	for {
		// Build / refresh round-robin order from current keys
		qMu.Lock()
		rrOrder = rrOrder[:0]
		for org, q := range queues {
			if len(q) > 0 {
				rrOrder = append(rrOrder, org)
			}
		}
		sort.Strings(rrOrder) // stable order; not required, but nice
		qMu.Unlock()

		if len(rrOrder) == 0 {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		// Pick next org (RR)
		org := rrOrder[rrIdx%len(rrOrder)]
		rrIdx++

		// Pop one candidate from that org
		var meta JobMeta
		qMu.Lock()
		q := queues[org]
		if len(q) == 0 {
			qMu.Unlock()
			continue
		}
		meta, queues[org] = q[0], q[1:]
		qMu.Unlock()

		// Attempt to claim atomically
		claimed, ok, err := claimJob(ctx, db, meta.ID)
		if err != nil {
			fmt.Println("claim error:", err)
			seen.Delete(meta.ID)
			continue
		}
		if !ok {
			// someone else (or earlier pass) changed status
			seen.Delete(meta.ID)
			continue
		}

		// Rate limit by provider
		provider := providerFromInput(claimed.Input)
		lim := providerLimiter[provider]
		if lim == nil {
			// Unknown provider → treat as very low rate
			lim = rate.NewLimiter(1, 1)
			providerLimiter[provider] = lim
		}

		<-inflightCh
		go func(j ClaimedJob) {
			defer func() { inflightCh <- struct{}{}; seen.Delete(j.ID) }()

			// Wait for provider token
			ctx2, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			if err := lim.Wait(ctx2); err != nil {
				// Couldn't obtain token; put back as pending (counts as an error attempt)
				_ = appendErrorOrRetry(ctx, db, j.ID, "provider limiter wait canceled")
				return
			}

			start := time.Now()
			// Simulate execution (sleep around 30s, 1% error)
			sleepDur, fail := simulateRun()
			select {
			case <-time.After(sleepDur):
			case <-ctx2.Done():
				fail = true
			}

			if fail {
				_ = appendErrorOrRetry(ctx, db, j.ID, fmt.Sprintf("simulated failure after %s", time.Since(start).Truncate(time.Millisecond)))
				return
			}

			// Success: write result JSON
			result := map[string]any{
				"ok":        true,
				"provider":  provider,
				"elapsedMs": time.Since(start).Milliseconds(),
			}
			resJSON, _ := json.Marshal(result)
			if err := markSucceeded(ctx, db, j.ID, string(resJSON)); err != nil {
				// If result write fails, treat as retryable
				_ = appendErrorOrRetry(ctx, db, j.ID, "post-success write failure: "+err.Error())
				return
			}
		}(claimed)
	}
}

// fetchCandidates pulls light candidates (no JSONB) for pending jobs.
// NOTE: We do NOT lease here; claim happens at dispatch via UPDATE ... RETURNING.
func fetchCandidates(ctx context.Context, db *pgxpool.Pool, n int) ([]JobMeta, error) {
	rows, err := db.Query(ctx, `
		SELECT id::text, org_id
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
		if err := rows.Scan(&m.ID, &m.Org); err != nil {
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

// ---------------- Simulation helpers ----------------

func simulateRun() (time.Duration, bool) {
	// ~Normal(30s, 6s^2) → clamp to [1s, 120s]
	mean := 30.0
	std := 6.0
	secs := mean + mathrand.NormFloat64()*std
	if secs < 1 {
		secs = 1
	}
	if secs > 120 {
		secs = 120
	}
	// 1% failure
	fail := mathrand.Intn(100) == 0
	return time.Duration(secs * float64(time.Second)), fail
}

func providerFromInput(b json.RawMessage) string {
	var tmp struct {
		Provider string `json:"provider"`
	}
	_ = json.Unmarshal(b, &tmp)
	if tmp.Provider == "" {
		return "unknown"
	}
	return strings.ToLower(tmp.Provider)
}

func randHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

// ----------------- tiny string-set -----------------

type stringSet struct {
	m sync.Map
}

func newStringSet() *stringSet { return &stringSet{} }

func (s *stringSet) Add(k string)      { s.m.Store(k, struct{}{}) }
func (s *stringSet) Has(k string) bool { _, ok := s.m.Load(k); return ok }
func (s *stringSet) Delete(k string)   { s.m.Delete(k) }
