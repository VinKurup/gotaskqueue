package gotaskqueue

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// --- Ring buffer mechanics (via the unexported dequeue) ---

func TestEnqueueDequeueRoundTrip(t *testing.T) {
	q := NewQueue("jobs")

	id, err := q.Enqueue("send_email", []byte("hello"))
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if id == "" {
		t.Fatal("Enqueue returned empty id")
	}

	got := q.dequeue()
	if got == nil {
		t.Fatal("dequeue returned nil on non-empty queue")
	}
	if got.ID != id || got.Type != "send_email" || string(got.Data) != "hello" {
		t.Fatalf("round-trip mismatch: got %+v", got)
	}
}

func TestFIFOOrder(t *testing.T) {
	q := NewQueue("jobs")
	types := []string{"a", "b", "c"}
	for _, ty := range types {
		if _, err := q.Enqueue(ty, nil); err != nil {
			t.Fatalf("Enqueue(%q) error: %v", ty, err)
		}
	}

	for _, want := range types {
		got := q.dequeue()
		if got == nil {
			t.Fatalf("dequeue returned nil, wanted %q", want)
		}
		if got.Type != want {
			t.Fatalf("FIFO order broken: got %q, want %q", got.Type, want)
		}
	}
}

func TestGrowBeyondBaseCapacity(t *testing.T) {
	q := NewQueue("jobs")
	const n = 50 // well past the base capacity of 8

	for i := 0; i < n; i++ {
		q.Enqueue(itoa(i), nil)
	}

	for i := 0; i < n; i++ {
		got := q.dequeue()
		if got == nil {
			t.Fatalf("dequeue %d returned nil after growth", i)
		}
		if got.Type != itoa(i) {
			t.Fatalf("order broken after growth at %d: got %q, want %q", i, got.Type, itoa(i))
		}
	}
}

func TestWraparound(t *testing.T) {
	q := NewQueue("jobs")

	// Fill, drain part way, then refill so tail wraps past head.
	for i := 0; i < 8; i++ {
		q.Enqueue(itoa(i), nil)
	}
	for i := 0; i < 5; i++ {
		got := q.dequeue()
		if got == nil || got.Type != itoa(i) {
			t.Fatalf("pre-wrap dequeue %d: %+v", i, got)
		}
	}
	for i := 8; i < 13; i++ {
		q.Enqueue(itoa(i), nil)
	}

	for i := 5; i < 13; i++ {
		got := q.dequeue()
		if got == nil {
			t.Fatalf("post-wrap dequeue %d returned nil", i)
		}
		if got.Type != itoa(i) {
			t.Fatalf("wraparound order broken at %d: got %q, want %q", i, got.Type, itoa(i))
		}
	}
	if q.dequeue() != nil {
		t.Fatal("queue should be empty after draining")
	}
}

func TestDequeueEmpty(t *testing.T) {
	q := NewQueue("jobs")
	if got := q.dequeue(); got != nil {
		t.Fatalf("dequeue on empty queue returned %+v", got)
	}
}

func TestConcurrentEnqueue(t *testing.T) {
	q := NewQueue("jobs")
	const n = 100

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			q.Enqueue("x", nil)
		}()
	}
	wg.Wait()

	count := 0
	for q.dequeue() != nil {
		count++
	}
	if count != n {
		t.Fatalf("concurrent enqueue lost items: got %d, want %d", count, n)
	}
}

// --- Status + worker loop ---

func TestEnqueueStartsPending(t *testing.T) {
	q := NewQueue("jobs")
	id, _ := q.Enqueue("send_email", nil)

	got, ok := q.GetTask(id)
	if !ok {
		t.Fatalf("GetTask(%q) not found", id)
	}
	if got.Status != StatusPending {
		t.Fatalf("new task status: got %q, want %q", got.Status, StatusPending)
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("new task has zero CreatedAt")
	}
}

func TestEnqueueEmptyType(t *testing.T) {
	q := NewQueue("jobs")
	if _, err := q.Enqueue("", nil); err == nil {
		t.Fatal("Enqueue with empty type should return an error")
	}
}

func TestProcessNextRunsHandler(t *testing.T) {
	q := NewQueue("jobs")

	var gotData string
	q.Register("greet", func(task Task) error {
		gotData = string(task.Data)
		return nil
	})

	id, _ := q.Enqueue("greet", []byte("hi"))

	processed, err := q.ProcessNext()
	if !processed {
		t.Fatal("ProcessNext reported nothing processed")
	}
	if err != nil {
		t.Fatalf("ProcessNext error: %v", err)
	}
	if gotData != "hi" {
		t.Fatalf("handler saw data %q, want %q", gotData, "hi")
	}

	got, _ := q.GetTask(id)
	if got.Status != StatusCompleted {
		t.Fatalf("task status after success: got %q, want %q", got.Status, StatusCompleted)
	}
}

func TestProcessNextHandlerError(t *testing.T) {
	q := NewQueue("jobs")
	boom := errors.New("boom")
	q.Register("fail", func(Task) error { return boom })

	id, _ := q.Enqueue("fail", nil)

	processed, err := q.ProcessNext()
	if !processed {
		t.Fatal("ProcessNext reported nothing processed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("ProcessNext error: got %v, want %v", err, boom)
	}

	got, _ := q.GetTask(id)
	if got.Status != StatusFailed {
		t.Fatalf("task status after handler error: got %q, want %q", got.Status, StatusFailed)
	}
}

func TestProcessNextNoHandler(t *testing.T) {
	q := NewQueue("jobs")
	id, _ := q.Enqueue("orphan", nil)

	processed, err := q.ProcessNext()
	if !processed {
		t.Fatal("ProcessNext reported nothing processed")
	}
	if err == nil {
		t.Fatal("ProcessNext should error when no handler is registered")
	}

	got, _ := q.GetTask(id)
	if got.Status != StatusFailed {
		t.Fatalf("task status with no handler: got %q, want %q", got.Status, StatusFailed)
	}
}

func TestProcessNextEmpty(t *testing.T) {
	q := NewQueue("jobs")
	processed, err := q.ProcessNext()
	if processed {
		t.Fatal("ProcessNext on empty queue should report processed=false")
	}
	if err != nil {
		t.Fatalf("ProcessNext on empty queue error: %v", err)
	}
}

// --- Background workers ---

func TestWorkersProcessAll(t *testing.T) {
	q := NewQueue("jobs")

	var count int64
	var wg sync.WaitGroup
	q.Register("job", func(Task) error {
		atomic.AddInt64(&count, 1)
		wg.Done()
		return nil
	})

	q.Start(3)

	const n = 20
	wg.Add(n)
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id, err := q.Enqueue("job", nil)
		if err != nil {
			t.Fatalf("Enqueue %d error: %v", i, err)
		}
		ids[i] = id
	}

	wg.Wait()
	q.Stop()

	if got := atomic.LoadInt64(&count); got != n {
		t.Fatalf("handler ran %d times, want %d", got, n)
	}
	for _, id := range ids {
		got, _ := q.GetTask(id)
		if got.Status != StatusCompleted {
			t.Fatalf("task %s status: got %q, want %q", id, got.Status, StatusCompleted)
		}
	}
}

func TestStopDrainsBacklog(t *testing.T) {
	q := NewQueue("jobs")

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var count int64
	q.Register("job", func(Task) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release // block until the test lets work proceed
		atomic.AddInt64(&count, 1)
		return nil
	})

	q.Start(1) // single worker, so the rest stay pending while task 1 is in-flight

	const n = 5
	for i := 0; i < n; i++ {
		if _, err := q.Enqueue("job", nil); err != nil {
			t.Fatalf("Enqueue %d error: %v", i, err)
		}
	}

	<-started // worker has task 1 in-flight; tasks 2..5 are pending

	// Stop blocks until the backlog is drained, so run it concurrently.
	done := make(chan struct{})
	go func() {
		q.Stop()
		close(done)
	}()

	close(release) // let all 5 drain
	<-done         // Stop returns only after the backlog is fully processed

	if got := atomic.LoadInt64(&count); got != n {
		t.Fatalf("graceful drain ran %d tasks, want %d", got, n)
	}
}

func TestEnqueueAfterStop(t *testing.T) {
	q := NewQueue("jobs")
	q.Start(1)
	q.Stop()

	if _, err := q.Enqueue("job", nil); !errors.Is(err, ErrQueueStopped) {
		t.Fatalf("Enqueue after Stop: got err %v, want %v", err, ErrQueueStopped)
	}
}

func TestStopIsIdempotent(t *testing.T) {
	q := NewQueue("jobs")
	q.Start(2)
	q.Stop()
	q.Stop() // must not panic or hang
}

// itoa is a tiny helper so tests don't depend on strconv for labels.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
