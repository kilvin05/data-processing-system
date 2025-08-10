package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Task struct {
	ID      int
	Payload string
}

type Result struct {
	WorkerID int
	TaskID   int
	Message  string
	Ts       time.Time
}

func worker(ctx context.Context, id int, tasks <-chan Task, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	logger := log.New(os.Stdout, fmt.Sprintf("[Worker-%d] ", id), log.LstdFlags|log.Lmicroseconds)
	logger.Printf("START")
	defer logger.Printf("COMPLETE")

	for {
		select {
		case <-ctx.Done():
			logger.Printf("Context cancelled, exiting")
			return
		case task, ok := <-tasks:
			if !ok {
				// channel closed, no more tasks
				return
			}
			if task.ID < 0 {
				logger.Printf("Received poison pill, exiting")
				return
			}

			// Simulate processing delay
			time.Sleep(time.Duration(50+rand.Intn(150)) * time.Millisecond)

			msg := fmt.Sprintf("processed task-%d payload='%s'", task.ID, task.Payload)
			results <- Result{WorkerID: id, TaskID: task.ID, Message: msg, Ts: time.Now()}
		}
	}
}

func resultWriter(results <-chan Result, outPath string, done chan<- error) {
	f, err := os.Create(outPath)
	if err != nil {
		done <- fmt.Errorf("create results file: %w", err)
		return
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			done <- fmt.Errorf("close results file: %w", cerr)
		} else {
			done <- err
		}
	}()

	w := bufio.NewWriter(f)
	defer w.Flush()

	for r := range results {
		line := fmt.Sprintf("Worker-%d processed task-%d at %s: %s\n",
			r.WorkerID, r.TaskID, r.Ts.Format("2006-01-02 15:04:05"), r.Message)
		if _, err := w.WriteString(line); err != nil {
			done <- fmt.Errorf("write results: %w", err)
			return
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	outDir := "out"
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("make out dir: %v", err)
	}
	resultFile := filepath.Join(outDir, "go_results.txt")

	logger := log.New(os.Stdout, "[DPS-Go] ", log.LstdFlags|log.Lmicroseconds)

	numWorkers := 4
	numTasks := 25

	tasks := make(chan Task)
	results := make(chan Result, 64)
	writerDone := make(chan error, 1)

	go resultWriter(results, resultFile, writerDone)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, i, tasks, results, &wg)
	}

	// Producer: send tasks
	go func() {
		defer close(tasks)
		for i := 1; i <= numTasks; i++ {
			tasks <- Task{ID: i, Payload: fmt.Sprintf("data-%d", i)}
		}
		// Send poison pills equal to worker count
		for i := 0; i < numWorkers; i++ {
			tasks <- Task{ID: -1, Payload: "POISON"}
		}
	}()

	// Wait for workers
	wg.Wait()
	// Close results so writer can finish
	close(results)

	// Wait for writer to finish and report error if any
	if err := <-writerDone; err != nil {
		logger.Printf("error: %v", err)
	} else {
		logger.Printf("Completed run. Results written to %s", resultFile)
	}

	fmt.Printf("GO DPS completed. See %s\n", resultFile)
}
