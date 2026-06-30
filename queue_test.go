package gotaskqueue

import (
	"sync"
	"testing"
)

func TestEnqueueDequeueRoundTrip(t *testing.T) {
	q := NewQueue("jobs")
	in := Task{ID: "1", Type: "send_email", Data: []byte("hello")}

	q.Enqueue(in)

	out, ok := q.Dequeue()
	if !ok {
		t.Fatal("Dequeue returned ok=false on non-empty queue")
	}
	if out.ID != in.ID || out.Type != in.Type || string(out.Data) != string(in.Data) {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", out, in)
	}
}

func TestFIFOOrder(t *testing.T) {
	q := NewQueue("jobs")
	ids := []string{"a", "b", "c"}
	for _, id := range ids {
		q.Enqueue(Task{ID: id})
	}

	for _, want := range ids {
		got, ok := q.Dequeue()
		if !ok {
			t.Fatalf("Dequeue returned ok=false, wanted %q", want)
		}
		if got.ID != want {
			t.Fatalf("FIFO order broken: got %q, want %q", got.ID, want)
		}
	}
}

func TestDequeueEmpty(t *testing.T) {
	q := NewQueue("jobs")

	got, ok := q.Dequeue()
	if ok {
		t.Fatalf("Dequeue on empty queue returned ok=true with %+v", got)
	}
	if got.ID != "" || got.Type != "" || got.Data != nil {
		t.Fatalf("Dequeue on empty queue returned non-zero Task: %+v", got)
	}
}

func TestGrowBeyondBaseCapacity(t *testing.T) {
	q := NewQueue("jobs")
	const n = 50 // well past the base capacity of 8

	for i := 0; i < n; i++ {
		q.Enqueue(Task{ID: string(rune('A' + i%26)), Type: itoa(i)})
	}

	for i := 0; i < n; i++ {
		got, ok := q.Dequeue()
		if !ok {
			t.Fatalf("Dequeue %d returned ok=false after growth", i)
		}
		if got.Type != itoa(i) {
			t.Fatalf("order broken after growth at %d: got Type %q, want %q", i, got.Type, itoa(i))
		}
	}
}

func TestWraparound(t *testing.T) {
	q := NewQueue("jobs")

	// Fill, drain part way, then refill so tail wraps past head.
	for i := 0; i < 8; i++ {
		q.Enqueue(Task{Type: itoa(i)})
	}
	for i := 0; i < 5; i++ {
		got, ok := q.Dequeue()
		if !ok || got.Type != itoa(i) {
			t.Fatalf("pre-wrap dequeue %d: ok=%v type=%q", i, ok, got.Type)
		}
	}
	for i := 8; i < 13; i++ {
		q.Enqueue(Task{Type: itoa(i)})
	}

	// Remaining should come out in FIFO order: 5..12
	for i := 5; i < 13; i++ {
		got, ok := q.Dequeue()
		if !ok {
			t.Fatalf("post-wrap dequeue %d returned ok=false", i)
		}
		if got.Type != itoa(i) {
			t.Fatalf("wraparound order broken at %d: got %q, want %q", i, got.Type, itoa(i))
		}
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("queue should be empty after draining")
	}
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

func TestConcurrentEnqueue(t *testing.T) {
	q := NewQueue("jobs")
	const n = 100

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			q.Enqueue(Task{ID: "x"})
		}()
	}
	wg.Wait()

	count := 0
	for {
		if _, ok := q.Dequeue(); !ok {
			break
		}
		count++
	}
	if count != n {
		t.Fatalf("concurrent enqueue lost items: got %d, want %d", count, n)
	}
}
