package gotaskqueue

import (
	"context"
	"testing"
)

// exercise runs the same logic against any Queue implementation.
func exercise(t *testing.T, q Queue) {
	t.Helper()

	var ran bool
	q.Register("job", func(_ context.Context, _ Task) error { ran = true; return nil })

	id, err := q.Enqueue("job", []byte("x"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	processed, err := q.ProcessNext()
	if err != nil {
		t.Fatalf("ProcessNext: %v", err)
	}
	if !processed {
		t.Fatal("nothing processed")
	}
	if !ran {
		t.Fatal("handler did not run")
	}

	got, ok, err := q.GetTask(id)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if !ok || got.Status != StatusCompleted {
		t.Fatalf("task status: %+v (ok=%v)", got, ok)
	}

	s, err := q.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Completed != 1 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestQueueInterface(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		exercise(t, NewMemoryQueue("jobs"))
	})
	t.Run("redis", func(t *testing.T) {
		exercise(t, NewRedisQueue(newTestRedis(t), "jobs"))
	})
}
