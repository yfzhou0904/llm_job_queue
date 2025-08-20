package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Constants for task management
const (
	// Progress percentages
	ProgressEnqueueJobs    = 5   // Initial job enqueue step
	ProgressProcessStart   = 10  // Start of job processing
	ProgressProcessMax     = 99  // Maximum progress during processing
	ProgressComplete       = 100 // Task completion
	
	// Job configuration
	DefaultMaxAttempts     = 3 // Default retry limit for jobs
	DefaultHexSeedLength   = 8 // Length of random hex seed
	
	// Monitoring
	TaskMonitorInterval    = 2 * time.Second // How often to check task progress
)

type TaskStep struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Percentage  int    `json:"percentage"`
}

type JobCounts struct {
	Total      int
	Done       int
	Failed     int
	Processing int
	Pending    int
}

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

	// Step 1: enqueueing jobs
	_ = setTaskCurrentStep(ctx, db, taskID, TaskStep{
		Name:        "enqueue_jobs",
		Description: fmt.Sprintf("Enqueuing %d jobs", count),
		Percentage:  ProgressEnqueueJobs,
	})

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
				"seed":      randHex(DefaultHexSeedLength),
			},
		}
		inJSON, _ := json.Marshal(jobInput)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jobs (org_id, task_id, type, input, status, max_attempts, created_at, updated_at)
			VALUES ($1, $2, 'sheet.enrich', $3::jsonb, 'pending', $4, $5, $5)
		`, orgID, taskID, string(inJSON), DefaultMaxAttempts, now); err != nil {
			return "", err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}

	// Step 2: processing
	_ = setTaskCurrentStep(ctx, db, taskID, TaskStep{
		Name:        "process_jobs",
		Description: "Processing enqueued jobs",
		Percentage:  ProgressProcessStart,
	})
	return taskID, nil
}

func getJobCounts(ctx context.Context, db *pgxpool.Pool, taskID string) (JobCounts, error) {
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
		return JobCounts{}, err
	}
	return JobCounts{
		Total:      total,
		Done:       done,
		Failed:     failed,
		Processing: processing,
		Pending:    total - done - failed - processing,
	}, nil
}

func calculateProgress(counts JobCounts) int {
	if counts.Total == 0 || counts.Done+counts.Failed >= counts.Total {
		return -1
	}
	frac := float64(counts.Done+counts.Failed) / float64(counts.Total)
	return int(math.Max(ProgressProcessStart, math.Min(ProgressProcessMax, math.Round(float64(ProgressProcessStart)+frac*float64(ProgressProcessMax-ProgressProcessStart)))))
}

func updateTaskProgress(ctx context.Context, db *pgxpool.Pool, taskID string, percentage int) error {
	return setTaskCurrentStep(ctx, db, taskID, TaskStep{
		Name:        "process_jobs",
		Description: "Processing enqueued jobs",
		Percentage:  percentage,
	})
}

func finalizeTask(ctx context.Context, db *pgxpool.Pool, taskID string, counts JobCounts) error {
	status := "completed"
	if counts.Done == 0 {
		status = "failed"
	}
	
	_, err := db.Exec(ctx, `
		UPDATE tasks
		SET status=$2, finished_at=now(), updated_at=now(), result=$3::jsonb
		WHERE id=$1
	`, taskID, status, fmt.Sprintf(`{"done":%d,"failed":%d}`, counts.Done, counts.Failed))
	if err != nil {
		return err
	}

	_ = setTaskCurrentStep(ctx, db, taskID, TaskStep{
		Name:        "finalize",
		Description: "Task finalized",
		Percentage:  ProgressComplete,
	})
	fmt.Println("[step] 100% - finalize")
	fmt.Println("Task finished with status:", status)
	return nil
}

func monitorTask(ctx context.Context, db *pgxpool.Pool, taskID string) {
	ticker := time.NewTicker(TaskMonitorInterval)
	defer ticker.Stop()
	start := time.Now()
	lastReported := -1
	
	for {
		<-ticker.C
		
		counts, err := getJobCounts(ctx, db, taskID)
		if err != nil {
			fmt.Println("monitor error:", err)
			continue
		}
		
		fmt.Printf("[monitor] elapsed=%s total=%d done=%d failed=%d processing=%d pending=%d\n",
			time.Since(start).Truncate(time.Second), counts.Total, counts.Done, 
			counts.Failed, counts.Processing, counts.Pending)

		pct := calculateProgress(counts)
		if pct > 0 && pct != lastReported {
			if err := updateTaskProgress(ctx, db, taskID, pct); err == nil {
				fmt.Printf("[step] %d%% - process_jobs\n", pct)
				lastReported = pct
			}
		}

		if counts.Total > 0 && counts.Done+counts.Failed == counts.Total {
			if err := finalizeTask(ctx, db, taskID, counts); err != nil {
				fmt.Println("finalize error:", err)
			}
			return
		}
	}
}

func setTaskCurrentStep(ctx context.Context, db *pgxpool.Pool, taskID string, step TaskStep) error {
	b, _ := json.Marshal(step)
	_, err := db.Exec(ctx, `
		UPDATE tasks
		SET current_step = $2::jsonb, updated_at = now()
		WHERE id = $1
	`, taskID, string(b))
	return err
}