package gotaskqueue

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// realRedis connects to an actual Redis server given by REDIS_ADDR, skipping the
// test when it is unset or unreachable. Run with, e.g.:
//
//	REDIS_ADDR=127.0.0.1:6399 go test -run Integration
func realRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping real-Redis integration test")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("cannot reach Redis at %s: %v", addr, err)
	}
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}
	return c
}

func TestIntegrationRedisSmoke(t *testing.T) {
	client := realRedis(t)
	q := NewRedisQueue(client, "smoke",
		WithRedisMaxRetries(3),
		WithRedisBackoff(5*time.Millisecond, 50*time.Millisecond),
		WithRedisPollInterval(10*time.Millisecond),
	)

	doneOK := make(chan struct{}, 100)
	q.Register("ok", func(_ context.Context, _ Task) error {
		doneOK <- struct{}{}
		return nil
	})

	var flapCalls int64
	q.Register("flap", func(_ context.Context, _ Task) error {
		if atomic.AddInt64(&flapCalls, 1) == 1 {
			return errors.New("first attempt fails")
		}
		doneOK <- struct{}{}
		return nil
	})

	entered := make(chan struct{})
	finished := make(chan struct{})
	q.Register("slow", func(ctx context.Context, _ Task) error {
		close(entered)
		<-ctx.Done()
		close(finished)
		return ctx.Err()
	})

	q.Start(4)
	defer q.Stop()

	const n = 10
	for i := 0; i < n; i++ {
		if _, err := q.Enqueue("ok", nil); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	delayedID, err := q.EnqueueDelayed("ok", nil, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("EnqueueDelayed: %v", err)
	}
	flapID, _ := q.Enqueue("flap", nil)
	slowID, _ := q.Enqueue("slow", nil)

	// n normal + 1 delayed + 1 flap (succeeds on retry) = n+2 completions
	for i := 0; i < n+2; i++ {
		select {
		case <-doneOK:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for completions (%d/%d)", i, n+2)
		}
	}

	<-entered
	if ok, err := q.Cancel(slowID); err != nil || !ok {
		t.Fatalf("Cancel(slow): ok=%v err=%v", ok, err)
	}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight handler was never cancelled")
	}

	check := func(id string, want TaskStatus) {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if got, ok, _ := q.GetTask(id); ok && got.Status == want {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		got, _, _ := q.GetTask(id)
		t.Fatalf("task %s: status %q, want %q", id, got.Status, want)
	}
	check(delayedID, StatusCompleted)
	check(flapID, StatusCompleted)
	check(slowID, StatusCancelled)

	if got := atomic.LoadInt64(&flapCalls); got != 2 {
		t.Fatalf("flap called %d times, want 2 (1 fail + 1 success)", got)
	}

	s, err := q.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Completed < n+2 {
		t.Fatalf("stats completed = %d, want >= %d", s.Completed, n+2)
	}
	if s.Cancelled != 1 {
		t.Fatalf("stats cancelled = %d, want 1", s.Cancelled)
	}
}
