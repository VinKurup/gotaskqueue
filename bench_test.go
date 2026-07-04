package gotaskqueue

import (
	"context"
	"testing"
)

func BenchmarkMemoryEnqueue(b *testing.B) {
	q := NewMemoryQueue("bench")
	data := []byte("payload")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue("job", data)
	}
}

func BenchmarkMemoryRoundTrip(b *testing.B) {
	q := NewMemoryQueue("bench")
	q.Register("job", func(context.Context, Task) error { return nil })
	data := []byte("payload")
	b.ReportAllocs()
	b.ResetTimer()
	// Enqueue then immediately process: measures the full path (enqueue,
	// dequeue, handler, status transitions). The registry grows over b.N.
	for i := 0; i < b.N; i++ {
		q.Enqueue("job", data)
		q.ProcessNext()
	}
}

func BenchmarkMemoryEnqueueParallel(b *testing.B) {
	q := NewMemoryQueue("bench")
	data := []byte("payload")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Enqueue("job", data)
		}
	})
}
