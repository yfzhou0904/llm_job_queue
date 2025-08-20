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
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Constants for magic numbers
const (
	// Simulation parameters
	SimulationMeanDuration  = 30.0  // seconds
	SimulationStdDev        = 6.0   // seconds
	SimulationMinDuration   = 1.0   // seconds
	SimulationMaxDuration   = 120.0 // seconds
	SimulationFailureRate   = 1     // 1% failure rate
	SimulationFailureChance = 100
)

var (
	mode         = flag.String("mode", "init", "init | worker | create-task")
	dsn          = flag.String("dsn", "", "Postgres DSN")
	orgFlag      = flag.String("org", "orgA", "Org ID (for create-task)")
	providerFlag = flag.String("provider", "openai", "Provider name for jobs (for create-task)")
	nJobs        = flag.Int("jobs", 50, "Number of jobs to create (for create-task)")

	// Worker tuning
	prefetchN  = flag.Int("prefetch", 1000, "Target number of prefetched candidate jobs")
	refillTrig = flag.Int("refill_trigger", 300, "Refill when in-memory candidates drop below this")
	inflight   = flag.Int("inflight", 400, "Max concurrent in-flight job executions")

	// Provider RPS overrides, e.g. -rps.openai=200 -rps.anthropic=100
	rpsOpenAI    = flag.Int("rps.openai", 120, "OpenAI RPS cap")
	rpsAnthropic = flag.Int("rps.anthropic", 80, "Anthropic RPS cap")
	rpsGemini    = flag.Int("rps.gemini", 60, "Gemini RPS cap")

	// Introspection / metrics (in-memory only)
	statsInterval = flag.Duration("stats_interval", 0, "Interval for worker stats logging (e.g. 5s, 1m); 0 to disable")
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
		handleInitMode(ctx, pool)
	case "create-task":
		handleCreateTaskMode(ctx, pool)
	case "worker":
		handleWorkerMode(ctx, pool)
	default:
		fmt.Println("Unknown -mode. Use init | worker | create-task")
	}
}

// ---------------- Mode Handlers ----------------

func handleInitMode(ctx context.Context, pool *pgxpool.Pool) {
	if err := initSchema(ctx, pool); err != nil {
		panic(err)
	}
	fmt.Println("Schema initialized.")
}

func handleCreateTaskMode(ctx context.Context, pool *pgxpool.Pool) {
	taskID, err := createTaskWithJobs(ctx, pool, *orgFlag, *providerFlag, *nJobs)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created task %s with %d jobs for org=%s provider=%s\n", taskID, *nJobs, *orgFlag, *providerFlag)
	monitorTask(ctx, pool, taskID)
}

func handleWorkerMode(ctx context.Context, pool *pgxpool.Pool) {
	if err := runWorker(ctx, pool); err != nil {
		panic(err)
	}
}

// ---------------- Worker (single machine) ----------------

// processingTracker keeps in-memory counts of currently-running jobs
type processingTracker struct {
	mu         sync.Mutex
	total      int
	byProvider map[string]int
	byOrg      map[string]int
}

func newProcessingTracker() *processingTracker {
	return &processingTracker{
		byProvider: map[string]int{},
		byOrg:      map[string]int{},
	}
}

func (t *processingTracker) add(provider, org string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.total++
	t.byProvider[provider]++
	t.byOrg[org]++
}

func (t *processingTracker) done(provider, org string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.total > 0 {
		t.total--
	}
	if t.byProvider[provider] > 0 {
		t.byProvider[provider]--
	}
	if t.byOrg[org] > 0 {
		t.byOrg[org]--
	}
}

// ---------------- Simulation helpers ----------------

func simulateRun() (time.Duration, bool) {
	// ~Normal(30s, 6s^2) → clamp to [1s, 120s]
	secs := SimulationMeanDuration + mathrand.NormFloat64()*SimulationStdDev
	if secs < SimulationMinDuration {
		secs = SimulationMinDuration
	}
	if secs > SimulationMaxDuration {
		secs = SimulationMaxDuration
	}
	// 1% failure rate
	fail := mathrand.Intn(SimulationFailureChance) < SimulationFailureRate
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
