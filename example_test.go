package gotaskqueue_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/VinKurup/gotaskqueue"
	"github.com/redis/go-redis/v9"
)

// Basic usage: register a handler, enqueue a job, run it.
func Example() {
	q := gotaskqueue.NewMemoryQueue("emails")

	q.Register("send", func(ctx context.Context, t gotaskqueue.Task) error {
		fmt.Printf("sent to %s\n", t.Data)
		return nil
	})

	id, _ := q.Enqueue("send", []byte("a@example.com"))
	q.ProcessNext() // run one job synchronously (no workers needed)

	task, _, _ := q.GetTask(id)
	fmt.Println("status:", task.Status)
	// Output:
	// sent to a@example.com
	// status: completed
}

// A job that exhausts its retries is dead-lettered; DeadLetters lists it and
// Replay requeues it.
func Example_deadLetterAndReplay() {
	q := gotaskqueue.NewMemoryQueue("jobs", gotaskqueue.WithMaxRetries(0))

	attempts := 0
	q.Register("flaky", func(ctx context.Context, t gotaskqueue.Task) error {
		attempts++
		if attempts == 1 {
			return errors.New("temporary failure")
		}
		return nil
	})

	id, _ := q.Enqueue("flaky", nil)
	q.ProcessNext() // fails -> dead-lettered

	dead, _ := q.DeadLetters()
	fmt.Println("dead letters:", len(dead))

	q.Replay(id)    // requeue
	q.ProcessNext() // succeeds on the second attempt

	task, _, _ := q.GetTask(id)
	fmt.Println("status:", task.Status)
	// Output:
	// dead letters: 1
	// status: completed
}

// EnqueueDelayed schedules a job to become runnable in the future.
func Example_delayed() {
	q := gotaskqueue.NewMemoryQueue("jobs")
	q.Register("later", func(ctx context.Context, t gotaskqueue.Task) error { return nil })

	id, _ := q.EnqueueDelayed("later", nil, time.Hour)

	task, _, _ := q.GetTask(id)
	fmt.Println("status:", task.Status) // waiting for its run time
	// Output:
	// status: scheduled
}

// The same code works against either backend via the Queue interface.
func Example_interface() {
	run := func(q gotaskqueue.Queue) {
		q.Register("job", func(ctx context.Context, t gotaskqueue.Task) error { return nil })
		q.Enqueue("job", nil)
		q.ProcessNext()
	}

	run(gotaskqueue.NewMemoryQueue("jobs"))
	fmt.Println("ok")
	// Output:
	// ok
}

// Redis backend: you supply the *redis.Client. Compiled as documentation; not
// run here (it would require a Redis server).
func ExampleNewRedisQueue() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	q := gotaskqueue.NewRedisQueue(client, "emails",
		gotaskqueue.WithRedisMaxRetries(5),
		gotaskqueue.WithRedisVisibilityTimeout(30*time.Second),
	)
	q.Register("send", func(ctx context.Context, t gotaskqueue.Task) error {
		return nil
	})

	q.Start(8)
	defer q.Stop()

	q.Enqueue("send", []byte("a@example.com"))
}
