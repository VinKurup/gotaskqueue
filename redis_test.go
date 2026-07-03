package gotaskqueue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func TestRedisEnqueueGetTask(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")

	id, err := q.Enqueue("send_email", []byte("hi"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if id == "" {
		t.Fatal("empty id")
	}

	got, ok, err := q.GetTask(id)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if !ok {
		t.Fatal("task not found")
	}
	if got.Type != "send_email" || string(got.Data) != "hi" || got.Status != StatusPending {
		t.Fatalf("bad task: %+v", got)
	}
}

func TestRedisEnqueueEmptyType(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	if _, err := q.Enqueue("", nil); err == nil {
		t.Fatal("expected error for empty type")
	}
}

func TestRedisProcessNext(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	var gotData string
	q.Register("greet", func(_ context.Context, task Task) error {
		gotData = string(task.Data)
		return nil
	})

	id, _ := q.Enqueue("greet", []byte("yo"))
	processed, err := q.ProcessNext()
	if err != nil {
		t.Fatalf("ProcessNext: %v", err)
	}
	if !processed {
		t.Fatal("nothing processed")
	}
	if gotData != "yo" {
		t.Fatalf("handler saw %q", gotData)
	}
	got, _, _ := q.GetTask(id)
	if got.Status != StatusCompleted {
		t.Fatalf("status: %q", got.Status)
	}
}

func TestRedisProcessNextEmpty(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	processed, err := q.ProcessNext()
	if err != nil {
		t.Fatalf("ProcessNext: %v", err)
	}
	if processed {
		t.Fatal("empty queue should report processed=false")
	}
}

func TestRedisFIFO(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	var order []string
	q.Register("t", func(_ context.Context, task Task) error {
		order = append(order, string(task.Data))
		return nil
	})

	for _, d := range []string{"a", "b", "c"} {
		q.Enqueue("t", []byte(d))
	}
	for i := 0; i < 3; i++ {
		q.ProcessNext()
	}
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Fatalf("FIFO broken: %v", order)
	}
}

func TestRedisHandlerErrorFails(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs", WithRedisMaxRetries(0))
	q.Register("bad", func(_ context.Context, _ Task) error { return errors.New("nope") })

	id, _ := q.Enqueue("bad", nil)
	processed, err := q.ProcessNext()
	if !processed {
		t.Fatal("nothing processed")
	}
	if err == nil {
		t.Fatal("expected handler error")
	}
	got, _, _ := q.GetTask(id)
	if got.Status != StatusFailed {
		t.Fatalf("status: %q", got.Status)
	}
}

func TestRedisNoHandlerFails(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	id, _ := q.Enqueue("orphan", nil)

	processed, err := q.ProcessNext()
	if !processed {
		t.Fatal("nothing processed")
	}
	if err == nil {
		t.Fatal("expected no-handler error")
	}
	got, _, _ := q.GetTask(id)
	if got.Status != StatusFailed {
		t.Fatalf("status: %q", got.Status)
	}
}

func waitForRedisStatus(t *testing.T, q *RedisQueue, id string, want TaskStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, ok, _ := q.GetTask(id); ok && got.Status == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	got, _, _ := q.GetTask(id)
	t.Fatalf("task %s: status %q did not reach %q within %v", id, got.Status, want, timeout)
}

func TestRedisEnqueueDelayedStartsScheduled(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	id, err := q.EnqueueDelayed("job", nil, time.Hour)
	if err != nil {
		t.Fatalf("EnqueueDelayed: %v", err)
	}
	got, ok, _ := q.GetTask(id)
	if !ok || got.Status != StatusScheduled {
		t.Fatalf("delayed task status: %q", got.Status)
	}
}

func TestRedisEnqueueDelayedRunsAfterDelay(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	const delay = 60 * time.Millisecond

	ran := make(chan time.Duration, 1)
	start := time.Now()
	q.Register("job", func(_ context.Context, _ Task) error {
		ran <- time.Since(start)
		return nil
	})

	q.Start(2)
	defer q.Stop()

	if _, err := q.EnqueueDelayed("job", nil, delay); err != nil {
		t.Fatalf("EnqueueDelayed: %v", err)
	}

	select {
	case elapsed := <-ran:
		if elapsed < delay {
			t.Fatalf("delayed task ran after %v, want >= %v", elapsed, delay)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("delayed task never ran")
	}
}

func TestRedisProcessNextPromotesDueDelayed(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	var ran bool
	q.Register("job", func(_ context.Context, _ Task) error { ran = true; return nil })

	q.EnqueueDelayed("job", nil, 0)

	processed, err := q.ProcessNext()
	if err != nil {
		t.Fatalf("ProcessNext: %v", err)
	}
	if !processed {
		t.Fatal("due delayed task was not promoted and run")
	}
	if !ran {
		t.Fatal("handler did not run")
	}
}

func TestRedisRetryEventuallyFails(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs",
		WithRedisMaxRetries(2), WithRedisBackoff(time.Millisecond, 5*time.Millisecond))
	var calls int64
	q.Register("flap", func(_ context.Context, _ Task) error {
		atomic.AddInt64(&calls, 1)
		return errors.New("nope")
	})

	q.Start(1)
	defer q.Stop()

	id, _ := q.Enqueue("flap", nil)
	waitForRedisStatus(t, q, id, StatusFailed, 3*time.Second)

	if got := atomic.LoadInt64(&calls); got != 3 {
		t.Fatalf("handler called %d times, want 3 (1 + 2 retries)", got)
	}
	if got, _, _ := q.GetTask(id); got.Retries != 2 {
		t.Fatalf("Retries = %d, want 2", got.Retries)
	}
}

func TestRedisRetrySucceedsAfterFailure(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs",
		WithRedisMaxRetries(3), WithRedisBackoff(time.Millisecond, 5*time.Millisecond))
	var calls int64
	q.Register("flap", func(_ context.Context, _ Task) error {
		if atomic.AddInt64(&calls, 1) == 1 {
			return errors.New("first fails")
		}
		return nil
	})

	q.Start(1)
	defer q.Stop()

	id, _ := q.Enqueue("flap", nil)
	waitForRedisStatus(t, q, id, StatusCompleted, 3*time.Second)

	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("handler called %d times, want 2", got)
	}
}

func TestRedisWorkers(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	var count int64
	var wg sync.WaitGroup
	q.Register("job", func(_ context.Context, _ Task) error {
		atomic.AddInt64(&count, 1)
		wg.Done()
		return nil
	})

	q.Start(3)
	defer q.Stop()

	const n = 20
	wg.Add(n)
	for i := 0; i < n; i++ {
		if _, err := q.Enqueue("job", nil); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	wg.Wait()

	if got := atomic.LoadInt64(&count); got != n {
		t.Fatalf("ran %d, want %d", got, n)
	}
}

func TestRedisStats(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs", WithRedisMaxRetries(0))
	q.Register("ok", func(_ context.Context, _ Task) error { return nil })
	q.Register("bad", func(_ context.Context, _ Task) error { return errors.New("x") })

	q.Enqueue("bad", nil)
	q.ProcessNext()
	q.Enqueue("ok", nil)
	q.ProcessNext()
	q.Enqueue("ok", nil)
	q.ProcessNext()
	q.Enqueue("ok", nil)
	q.EnqueueDelayed("ok", nil, time.Hour)

	s, err := q.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Total != 5 || s.Completed != 2 || s.Failed != 1 || s.Pending != 1 || s.Scheduled != 1 {
		t.Fatalf("counts wrong: %+v", s)
	}
}

func TestRedisCancelPending(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	var ran bool
	q.Register("job", func(_ context.Context, _ Task) error { ran = true; return nil })

	id, _ := q.Enqueue("job", nil)
	ok, err := q.Cancel(id)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !ok {
		t.Fatal("Cancel(pending) returned false")
	}
	if got, _, _ := q.GetTask(id); got.Status != StatusCancelled {
		t.Fatalf("status: %q", got.Status)
	}

	processed, _ := q.ProcessNext()
	if processed {
		t.Fatal("cancelled task should be skipped by ProcessNext")
	}
	if ran {
		t.Fatal("cancelled task's handler ran")
	}
}

func TestRedisCancelInFlight(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs",
		WithRedisMaxRetries(3), WithRedisPollInterval(5*time.Millisecond))

	entered := make(chan struct{})
	finished := make(chan struct{})
	var ctxErr error
	q.Register("slow", func(ctx context.Context, _ Task) error {
		close(entered)
		<-ctx.Done()
		ctxErr = ctx.Err()
		close(finished)
		return ctx.Err()
	})

	q.Start(1)
	defer q.Stop()

	id, _ := q.Enqueue("slow", nil)
	<-entered

	ok, err := q.Cancel(id)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !ok {
		t.Fatal("Cancel on a running task returned false")
	}

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was never cancelled")
	}
	if ctxErr == nil {
		t.Fatal("handler's context was not cancelled")
	}

	waitForRedisStatus(t, q, id, StatusCancelled, 2*time.Second)

	time.Sleep(30 * time.Millisecond)
	if got, _, _ := q.GetTask(id); got.Status != StatusCancelled {
		t.Fatalf("cancelled in-flight task was retried/changed: %q", got.Status)
	}
}

func TestRedisCancelScheduled(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	id, _ := q.EnqueueDelayed("job", nil, time.Hour)

	ok, err := q.Cancel(id)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !ok {
		t.Fatal("Cancel(scheduled) returned false")
	}
	if got, _, _ := q.GetTask(id); got.Status != StatusCancelled {
		t.Fatalf("status: %q", got.Status)
	}
}

func TestRedisCancelCompletedReturnsFalse(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	q.Register("job", func(_ context.Context, _ Task) error { return nil })
	id, _ := q.Enqueue("job", nil)
	q.ProcessNext()

	ok, err := q.Cancel(id)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if ok {
		t.Fatal("Cancel on completed task should return false")
	}
}

func TestRedisWorkerSkipsCancelled(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs")
	var ranCancelled int64
	done := make(chan struct{})
	q.Register("cancel", func(_ context.Context, _ Task) error { atomic.AddInt64(&ranCancelled, 1); return nil })
	q.Register("keep", func(_ context.Context, _ Task) error { close(done); return nil })

	cancelID, _ := q.Enqueue("cancel", nil)
	q.Enqueue("keep", nil)
	if _, err := q.Cancel(cancelID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	q.Start(1)
	defer q.Stop()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second task never ran")
	}
	if got := atomic.LoadInt64(&ranCancelled); got != 0 {
		t.Fatalf("cancelled handler ran %d times, want 0", got)
	}
}

func TestRedisCleanup(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs", WithRedisTaskTTL(time.Millisecond))
	q.Register("ok", func(_ context.Context, _ Task) error { return nil })

	id, _ := q.Enqueue("ok", nil)
	q.ProcessNext()
	if got, _, _ := q.GetTask(id); got.Status != StatusCompleted {
		t.Fatalf("not completed: %q", got.Status)
	}

	time.Sleep(5 * time.Millisecond)

	n, err := q.Cleanup()
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if n != 1 {
		t.Fatalf("Cleanup purged %d, want 1", n)
	}
	if _, ok, _ := q.GetTask(id); ok {
		t.Fatal("task still present after cleanup")
	}
}
