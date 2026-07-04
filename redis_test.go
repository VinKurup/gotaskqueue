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

func TestRedisEnqueueRegistersAndQueuesTogether(t *testing.T) {
	client := newTestRedis(t)
	q := NewRedisQueue(client, "jobs")
	ctx := context.Background()

	id, err := q.Enqueue("job", []byte("x"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if got, ok, _ := q.GetTask(id); !ok || got.Status != StatusPending {
		t.Fatalf("task not registered as pending: ok=%v status=%q", ok, got.Status)
	}
	ready, _ := client.LRange(ctx, q.readyKey(), 0, -1).Result()
	if !contains(ready, id) {
		t.Fatalf("id %s not in ready list %v", id, ready)
	}

	did, err := q.EnqueueDelayed("job", nil, time.Hour)
	if err != nil {
		t.Fatalf("EnqueueDelayed: %v", err)
	}
	if got, ok, _ := q.GetTask(did); !ok || got.Status != StatusScheduled {
		t.Fatalf("delayed task not registered as scheduled: ok=%v status=%q", ok, got.Status)
	}
	sched, _ := client.ZRange(ctx, q.delayedKey(), 0, -1).Result()
	if !contains(sched, did) {
		t.Fatalf("id %s not in delayed set %v", did, sched)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
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

func TestRedisPromoteClaimsOnce(t *testing.T) {
	client := newTestRedis(t)
	q := NewRedisQueue(client, "jobs")
	ctx := context.Background()

	id, _ := q.EnqueueDelayed("job", nil, 0) // due immediately, sitting in delayed

	// Two promoters racing on the same due task: the claim must let only one move it.
	p1, _ := promoteScript.Run(ctx, client, []string{q.delayedKey(), q.readyKey()}, id).Int()
	p2, _ := promoteScript.Run(ctx, client, []string{q.delayedKey(), q.readyKey()}, id).Int()
	if p1 != 1 || p2 != 0 {
		t.Fatalf("claim: p1=%d p2=%d, want 1 and 0", p1, p2)
	}

	ready, _ := client.LRange(ctx, q.readyKey(), 0, -1).Result()
	n := 0
	for _, v := range ready {
		if v == id {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("id promoted %d times, want exactly 1", n)
	}
}

func TestRedisRetryReschedulesAtomically(t *testing.T) {
	client := newTestRedis(t)
	q := NewRedisQueue(client, "jobs", WithRedisMaxRetries(3), WithRedisBackoff(time.Hour, time.Hour))
	q.Register("bad", func(_ context.Context, _ Task) error { return errors.New("nope") })

	id, _ := q.Enqueue("bad", nil)
	q.ProcessNext() // handler errors -> retryOrFail reschedules

	got, _, _ := q.GetTask(id)
	if got.Status != StatusScheduled {
		t.Fatalf("status %q, want scheduled", got.Status)
	}
	if got.Retries != 1 {
		t.Fatalf("retries %d, want 1", got.Retries)
	}
	sched, _ := client.ZRange(context.Background(), q.delayedKey(), 0, -1).Result()
	if !contains(sched, id) {
		t.Fatalf("rescheduled task not in delayed set: %v", sched)
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

func TestRedisReaperRedeliversStranded(t *testing.T) {
	client := newTestRedis(t)
	q := NewRedisQueue(client, "jobs",
		WithRedisVisibilityTimeout(20*time.Millisecond),
		WithRedisPollInterval(5*time.Millisecond))

	done := make(chan struct{}, 1)
	var ran int64
	q.Register("job", func(_ context.Context, _ Task) error {
		atomic.AddInt64(&ran, 1)
		done <- struct{}{}
		return nil
	})

	id, _ := q.Enqueue("job", nil)

	// Simulate a worker that claimed the job then crashed: move it into the
	// in-flight list with an already-expired deadline, and never process it.
	ctx := context.Background()
	if _, err := client.RPopLPush(ctx, q.readyKey(), q.inflightKey()).Result(); err != nil {
		t.Fatalf("setup RPOPLPUSH: %v", err)
	}
	client.ZAdd(ctx, q.deadlineKey(), redis.Z{Score: unixScore(time.Now().Add(-time.Second)), Member: id})

	q.Start(1)
	defer q.Stop()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stranded task was never redelivered")
	}
	waitForRedisStatus(t, q, id, StatusCompleted, 2*time.Second)
	if got := atomic.LoadInt64(&ran); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
}

func TestRedisReaperRecoversOrphanWithoutDeadline(t *testing.T) {
	client := newTestRedis(t)
	q := NewRedisQueue(client, "jobs", WithRedisVisibilityTimeout(time.Millisecond))
	ctx := context.Background()

	id, _ := q.Enqueue("job", nil)

	// Simulate a worker that claimed the job (atomic move to inflight) then died
	// before setting a deadline — the gap the reconciliation must cover.
	client.RPopLPush(ctx, q.readyKey(), q.inflightKey())
	if _, err := client.ZScore(ctx, q.deadlineKey(), id).Result(); err != redis.Nil {
		t.Fatalf("precondition: expected no deadline for orphan")
	}

	q.reap(ctx) // reconcile: assigns a deadline so the orphan is visible
	if _, err := client.ZScore(ctx, q.deadlineKey(), id).Result(); err != nil {
		t.Fatalf("reconcile should have assigned a deadline: %v", err)
	}

	time.Sleep(3 * time.Millisecond)
	q.reap(ctx) // deadline now expired: redeliver

	ready, _ := client.LRange(ctx, q.readyKey(), 0, -1).Result()
	if !contains(ready, id) {
		t.Fatalf("orphan was not redelivered to ready: %v", ready)
	}
	infl, _ := client.LRange(ctx, q.inflightKey(), 0, -1).Result()
	if contains(infl, id) {
		t.Fatalf("orphan still in inflight after redelivery: %v", infl)
	}
}

func TestRedisInflightClearedAfterCompletion(t *testing.T) {
	client := newTestRedis(t)
	q := NewRedisQueue(client, "jobs", WithRedisPollInterval(5*time.Millisecond))
	done := make(chan struct{}, 1)
	q.Register("job", func(_ context.Context, _ Task) error { done <- struct{}{}; return nil })

	q.Start(1)
	defer q.Stop()
	q.Enqueue("job", nil)
	<-done

	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		l, _ := client.LLen(ctx, q.inflightKey()).Result()
		z, _ := client.ZCard(ctx, q.deadlineKey()).Result()
		if l == 0 && z == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	l, _ := client.LLen(ctx, q.inflightKey()).Result()
	z, _ := client.ZCard(ctx, q.deadlineKey()).Result()
	t.Fatalf("in-flight not cleared after completion: inflight=%d deadlines=%d", l, z)
}

func TestRedisReaperPoisonCap(t *testing.T) {
	client := newTestRedis(t)
	q := NewRedisQueue(client, "jobs", WithRedisMaxDeliveries(1), WithRedisVisibilityTimeout(time.Millisecond))
	id, _ := q.Enqueue("job", nil)
	ctx := context.Background()

	strand := func() {
		client.RPopLPush(ctx, q.readyKey(), q.inflightKey())
		client.ZAdd(ctx, q.deadlineKey(), redis.Z{Score: unixScore(time.Now().Add(-time.Second)), Member: id})
	}

	strand()
	q.reap(ctx) // 1st redelivery: Deliveries 0->1, still <= max
	if got, _, _ := q.GetTask(id); got.Status != StatusPending {
		t.Fatalf("after 1st reap: status %q, want pending", got.Status)
	}
	strand()
	q.reap(ctx) // 2nd: Deliveries 1->2 > max -> poison -> failed
	if got, _, _ := q.GetTask(id); got.Status != StatusFailed {
		t.Fatalf("after 2nd reap: status %q, want failed", got.Status)
	}
}

func TestRedisReaperDoesNotBurnRetryBudget(t *testing.T) {
	client := newTestRedis(t)
	q := NewRedisQueue(client, "jobs",
		WithRedisMaxRetries(3), WithRedisMaxDeliveries(5), WithRedisVisibilityTimeout(time.Millisecond))
	id, _ := q.Enqueue("job", nil)
	ctx := context.Background()

	client.RPopLPush(ctx, q.readyKey(), q.inflightKey())
	client.ZAdd(ctx, q.deadlineKey(), redis.Z{Score: unixScore(time.Now().Add(-time.Second)), Member: id})
	q.reap(ctx)

	got, _, _ := q.GetTask(id)
	if got.Retries != 0 {
		t.Fatalf("Retries=%d; a crash redelivery must not consume the handler-retry budget", got.Retries)
	}
	if got.Deliveries != 1 {
		t.Fatalf("Deliveries=%d, want 1", got.Deliveries)
	}
	if got.Status != StatusPending {
		t.Fatalf("status %q, want pending", got.Status)
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

func TestRedisBoundedShutdown(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs",
		WithRedisShutdownTimeout(50*time.Millisecond), WithRedisPollInterval(5*time.Millisecond))

	entered := make(chan struct{})
	block := make(chan struct{})
	q.Register("stuck", func(_ context.Context, _ Task) error {
		close(entered)
		<-block // ignores ctx, blocks forever
		return nil
	})

	q.Start(1)
	q.Enqueue("stuck", nil)
	<-entered

	done := make(chan struct{})
	go func() { q.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within the shutdown timeout despite a stuck handler")
	}

	close(block)
	time.Sleep(50 * time.Millisecond) // let the worker drain before miniredis closes
}

func TestRedisHandlerTimeout(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs",
		WithRedisHandlerTimeout(20*time.Millisecond), WithRedisMaxRetries(0),
		WithRedisPollInterval(5*time.Millisecond))

	fired := make(chan error, 1)
	q.Register("slow", func(ctx context.Context, _ Task) error {
		<-ctx.Done()
		fired <- ctx.Err()
		return ctx.Err()
	})

	q.Start(1)
	defer q.Stop()

	id, _ := q.Enqueue("slow", nil)

	select {
	case err := <-fired:
		if err != context.DeadlineExceeded {
			t.Fatalf("handler ctx error = %v, want DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler was never timed out")
	}

	waitForRedisStatus(t, q, id, StatusFailed, 2*time.Second)
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

func TestRedisMaxTasksEvictsOldestTerminal(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs", WithRedisMaxTasks(2), WithRedisTaskTTL(time.Hour))
	q.Register("ok", func(_ context.Context, _ Task) error { return nil })

	ids := make([]string, 5)
	for i := 0; i < 5; i++ {
		ids[i], _ = q.Enqueue("ok", nil)
		q.ProcessNext()
		time.Sleep(time.Millisecond) // distinct FinishedAt for deterministic ordering
	}

	n, err := q.Cleanup()
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if n != 3 {
		t.Fatalf("evicted %d, want 3", n)
	}
	for _, id := range ids[:3] {
		if _, ok, _ := q.GetTask(id); ok {
			t.Fatalf("old task %s should have been evicted", id)
		}
	}
	for _, id := range ids[3:] {
		if _, ok, _ := q.GetTask(id); !ok {
			t.Fatalf("recent task %s should have been kept", id)
		}
	}
}

func TestRedisMaxTasksKeepsLive(t *testing.T) {
	q := NewRedisQueue(newTestRedis(t), "jobs", WithRedisMaxTasks(1), WithRedisTaskTTL(time.Hour))
	q.Register("ok", func(_ context.Context, _ Task) error { return nil })

	done, _ := q.Enqueue("ok", nil)
	q.ProcessNext()
	p1, _ := q.Enqueue("ok", nil)
	p2, _ := q.Enqueue("ok", nil)

	q.Cleanup()

	if _, ok, _ := q.GetTask(done); ok {
		t.Fatal("completed task should have been evicted")
	}
	for _, id := range []string{p1, p2} {
		if _, ok, _ := q.GetTask(id); !ok {
			t.Fatalf("pending task %s must not be evicted", id)
		}
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
