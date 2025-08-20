package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"
)

// Constants for worker operations
const (
	// Worker timing
	RefillLoopInterval    = 1000 * time.Millisecond // How often to check for refill
	DispatchSleepDuration = 20 * time.Millisecond   // Sleep when no work available
	JobProcessingTimeout  = 3 * time.Minute         // Timeout for job processing

	// Worker thresholds
	MinRefillAmount     = 100 // Minimum jobs to fetch on refill
	DefaultRateLimitRPS = 1   // Default RPS when provider not configured
)

type WorkerConfig struct {
	PrefetchN     int
	RefillTrig    int
	Inflight      int
	StatsInterval time.Duration
	ProviderRPS   map[string]int
}

type Worker struct {
	db     *pgxpool.Pool
	config WorkerConfig

	providerLimiter   map[string]*rate.Limiter
	inflightCh        chan struct{}
	queues            map[string][]JobMeta
	queuesMutex       sync.Mutex
	seen              *stringSet
	processingTracker *processingTracker
	statsFormatter    *StatsFormatter

	rrOrder []string
	rrIdx   int
}

func NewWorker(db *pgxpool.Pool, config WorkerConfig) *Worker {
	w := &Worker{
		db:                db,
		config:            config,
		queues:            make(map[string][]JobMeta),
		seen:              newStringSet(),
		processingTracker: newProcessingTracker(),
		statsFormatter:    NewStatsFormatter(),
	}

	w.setupProviderLimiters()
	w.setupInflightLimiter()

	return w
}

func (w *Worker) setupProviderLimiters() {
	w.providerLimiter = make(map[string]*rate.Limiter)
	for provider, rps := range w.config.ProviderRPS {
		if rps < 1 {
			rps = 1
		}
		w.providerLimiter[provider] = rate.NewLimiter(rate.Limit(rps), rps)
	}
}

func (w *Worker) setupInflightLimiter() {
	w.inflightCh = make(chan struct{}, w.config.Inflight)
	for i := 0; i < w.config.Inflight; i++ {
		w.inflightCh <- struct{}{}
	}
}

func (w *Worker) Run(ctx context.Context) error {
	go w.runRefillLoop(ctx)

	if w.config.StatsInterval > 0 {
		go w.runStatsLoop(ctx)
	}

	w.runDispatchLoop(ctx)
	return nil
}

func (w *Worker) runRefillLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(RefillLoopInterval):
		}

		if !w.needsRefill() {
			continue
		}

		want := w.calculateRefillAmount()
		metas, err := fetchCandidates(ctx, w.db, want)
		if err != nil {
			fmt.Println("refill fetch error:", err)
			continue
		}
		if len(metas) == 0 {
			continue
		}

		w.addCandidatesToQueues(metas)
	}
}

func (w *Worker) needsRefill() bool {
	w.queuesMutex.Lock()
	defer w.queuesMutex.Unlock()

	totalCandidates := 0
	for _, q := range w.queues {
		totalCandidates += len(q)
	}

	return totalCandidates < w.config.RefillTrig
}

func (w *Worker) calculateRefillAmount() int {
	w.queuesMutex.Lock()
	defer w.queuesMutex.Unlock()

	totalCandidates := 0
	for _, q := range w.queues {
		totalCandidates += len(q)
	}

	want := w.config.PrefetchN - totalCandidates
	if want < MinRefillAmount {
		want = MinRefillAmount
	}
	return want
}

func (w *Worker) addCandidatesToQueues(metas []JobMeta) {
	w.queuesMutex.Lock()
	defer w.queuesMutex.Unlock()

	for _, m := range metas {
		if w.seen.Has(m.ID) {
			continue
		}
		w.seen.Add(m.ID)
		w.queues[m.Org] = append(w.queues[m.Org], m)
	}
}

func (w *Worker) runStatsLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.StatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.printStats()
		}
	}
}

func (w *Worker) printStats() {
	queueStats := w.getQueueStats()
	processingStats := w.getProcessingStats()
	inflightStats := w.getInflightStats()

	var b strings.Builder
	fmt.Fprintf(&b, "[stats] %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(&b, "  inflight: used=%d free=%d cap=%d\n",
		inflightStats.Used, inflightStats.Free, inflightStats.Cap)

	w.formatProcessingStats(&b, processingStats)
	w.formatQueueStats(&b, queueStats)

	fmt.Print(b.String())
}

type InflightStats struct {
	Used, Free, Cap int
}

type ProcessingStats struct {
	Total      int
	ByProvider map[string]int
	ByOrg      map[string]int
}

type QueueStats struct {
	Total      int
	PerOrg     map[string]int
	PerOrgType map[string]map[string]int
}

func (w *Worker) getInflightStats() InflightStats {
	cap := cap(w.inflightCh)
	free := len(w.inflightCh)
	used := cap - free
	return InflightStats{Used: used, Free: free, Cap: cap}
}

func (w *Worker) getProcessingStats() ProcessingStats {
	w.processingTracker.mu.Lock()
	defer w.processingTracker.mu.Unlock()

	byProvider := make(map[string]int, len(w.processingTracker.byProvider))
	for k, v := range w.processingTracker.byProvider {
		byProvider[k] = v
	}

	byOrg := make(map[string]int, len(w.processingTracker.byOrg))
	for k, v := range w.processingTracker.byOrg {
		byOrg[k] = v
	}

	return ProcessingStats{
		Total:      w.processingTracker.total,
		ByProvider: byProvider,
		ByOrg:      byOrg,
	}
}

func (w *Worker) getQueueStats() QueueStats {
	w.queuesMutex.Lock()
	defer w.queuesMutex.Unlock()

	total := 0
	perOrg := make(map[string]int)
	perOrgType := make(map[string]map[string]int)

	for org, q := range w.queues {
		perOrg[org] = len(q)
		total += len(q)

		if perOrgType[org] == nil {
			perOrgType[org] = make(map[string]int)
		}
		for _, jm := range q {
			perOrgType[org][jm.Type]++
		}
	}

	return QueueStats{
		Total:      total,
		PerOrg:     perOrg,
		PerOrgType: perOrgType,
	}
}

type StatsFormatter struct{}

func NewStatsFormatter() *StatsFormatter {
	return &StatsFormatter{}
}

func (sf *StatsFormatter) formatKeyValueMap(b *strings.Builder, m map[string]int, label string) {
	if len(m) == 0 {
		return
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Fprintf(b, " %s=[", label)
	for i, k := range keys {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(b, "%s:%d", k, m[k])
	}
	b.WriteString("]")
}

func (sf *StatsFormatter) FormatProcessingStats(b *strings.Builder, stats ProcessingStats) {
	fmt.Fprintf(b, "  processing: total=%d", stats.Total)
	sf.formatKeyValueMap(b, stats.ByProvider, "by_provider")
	sf.formatKeyValueMap(b, stats.ByOrg, "by_org")
	b.WriteByte('\n')
}

func (sf *StatsFormatter) FormatQueueStats(b *strings.Builder, stats QueueStats) {
	fmt.Fprintf(b, "  queued: total=%d\n", stats.Total)

	if len(stats.PerOrg) == 0 {
		return
	}

	orgs := make([]string, 0, len(stats.PerOrg))
	for k := range stats.PerOrg {
		orgs = append(orgs, k)
	}
	sort.Strings(orgs)

	for _, org := range orgs {
		fmt.Fprintf(b, "    - org=%s count=%d", org, stats.PerOrg[org])
		types := stats.PerOrgType[org]
		if len(types) > 0 {
			typeKeys := make([]string, 0, len(types))
			for tk := range types {
				typeKeys = append(typeKeys, tk)
			}
			sort.Strings(typeKeys)
			b.WriteString(" types=[")
			for i, tk := range typeKeys {
				if i > 0 {
					b.WriteString(" ")
				}
				fmt.Fprintf(b, "%s:%d", tk, types[tk])
			}
			b.WriteString("]")
		}
		b.WriteByte('\n')
	}
}

func (w *Worker) formatProcessingStats(b *strings.Builder, stats ProcessingStats) {
	w.statsFormatter.FormatProcessingStats(b, stats)
}

func (w *Worker) formatQueueStats(b *strings.Builder, stats QueueStats) {
	w.statsFormatter.FormatQueueStats(b, stats)
}

func (w *Worker) runDispatchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !w.updateRoundRobinOrder() {
			time.Sleep(DispatchSleepDuration)
			continue
		}

		org := w.getNextOrg()
		meta, ok := w.popCandidateFromOrg(org)
		if !ok {
			continue
		}

		claimed, ok, err := claimJob(ctx, w.db, meta.ID)
		if err != nil {
			fmt.Println("claim error:", err)
			w.seen.Delete(meta.ID)
			continue
		}
		if !ok {
			w.seen.Delete(meta.ID)
			continue
		}

		w.processJobAsync(ctx, claimed)
	}
}

func (w *Worker) updateRoundRobinOrder() bool {
	w.queuesMutex.Lock()
	defer w.queuesMutex.Unlock()

	w.rrOrder = w.rrOrder[:0]
	for org, q := range w.queues {
		if len(q) > 0 {
			w.rrOrder = append(w.rrOrder, org)
		}
	}
	sort.Strings(w.rrOrder)

	return len(w.rrOrder) > 0
}

func (w *Worker) getNextOrg() string {
	org := w.rrOrder[w.rrIdx%len(w.rrOrder)]
	w.rrIdx++
	return org
}

func (w *Worker) popCandidateFromOrg(org string) (JobMeta, bool) {
	w.queuesMutex.Lock()
	defer w.queuesMutex.Unlock()

	q := w.queues[org]
	if len(q) == 0 {
		return JobMeta{}, false
	}

	meta := q[0]
	w.queues[org] = q[1:]
	return meta, true
}

func (w *Worker) processJobAsync(ctx context.Context, claimed ClaimedJob) {
	provider := providerFromInput(claimed.Input)
	lim := w.getProviderLimiter(provider)

	<-w.inflightCh
	go w.processJob(ctx, claimed, provider, lim)
}

func (w *Worker) getProviderLimiter(provider string) *rate.Limiter {
	lim := w.providerLimiter[provider]
	if lim == nil {
		lim = rate.NewLimiter(DefaultRateLimitRPS, DefaultRateLimitRPS)
		w.providerLimiter[provider] = lim
	}
	return lim
}

func (w *Worker) processJob(ctx context.Context, job ClaimedJob, provider string, lim *rate.Limiter) {
	defer func() {
		w.inflightCh <- struct{}{}
		w.seen.Delete(job.ID)
		w.processingTracker.done(provider, job.Org)
	}()

	ctx2, cancel := context.WithTimeout(ctx, JobProcessingTimeout)
	defer cancel()

	if err := lim.Wait(ctx2); err != nil {
		_ = appendErrorOrRetry(ctx, w.db, job.ID, "provider limiter wait canceled")
		return
	}

	w.processingTracker.add(provider, job.Org)

	start := time.Now()
	sleepDur, fail := simulateRun()
	select {
	case <-time.After(sleepDur):
	case <-ctx2.Done():
		fail = true
	}

	if fail {
		errMsg := fmt.Sprintf("simulated failure after %s", time.Since(start).Truncate(time.Millisecond))
		_ = appendErrorOrRetry(ctx, w.db, job.ID, errMsg)
		return
	}

	result := map[string]any{
		"ok":        true,
		"provider":  provider,
		"elapsedMs": time.Since(start).Milliseconds(),
	}
	resJSON, _ := json.Marshal(result)
	if err := markSucceeded(ctx, w.db, job.ID, string(resJSON)); err != nil {
		_ = appendErrorOrRetry(ctx, w.db, job.ID, "post-success write failure: "+err.Error())
		return
	}
}

func runWorker(ctx context.Context, db *pgxpool.Pool) error {
	config := WorkerConfig{
		PrefetchN:     *prefetchN,
		RefillTrig:    *refillTrig,
		Inflight:      *inflight,
		StatsInterval: *statsInterval,
		ProviderRPS: map[string]int{
			"openai":    *rpsOpenAI,
			"anthropic": *rpsAnthropic,
			"gemini":    *rpsGemini,
		},
	}

	worker := NewWorker(db, config)
	return worker.Run(ctx)
}
