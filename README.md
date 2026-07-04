# gotaskqueue

[![CI](https://github.com/VinKurup/gotaskqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/VinKurup/gotaskqueue/actions/workflows/ci.yml)

A task/job queue for Go with two interchangeable backends behind one interface:

- **`MemoryQueue`**: in-process, zero dependencies. Fast, ephemeral. Good for a
  single process, tests, and small tools.
- **`RedisQueue`**: backed by Redis. Shared across processes/machines, survives
  restarts, **at-least-once** delivery with crash recovery.

Both satisfy the same `Queue` interface, so you can develop against the memory
queue and swap in Redis for production without changing call sites.

```go
func process(q gotaskqueue.Queue) { /* works with either backend */ }
```

## Features

- FIFO ordering with a pool of background workers
- Delayed jobs (`EnqueueDelayed`) and automatic retries with exponential backoff
- Cancellation — of queued jobs and, cooperatively, of running ones
- Per-handler execution timeouts and bounded graceful shutdown
- Dead-letter queue: failed jobs are durable, inspectable, and replayable
- Status tracking, aggregate stats, TTL + max-count cleanup
- Redis backend adds: at-least-once delivery, crash recovery (visibility
  timeout + reaper), and poison-message capping — all multi-process safe
- Pluggable error logging (`*slog.Logger`-compatible)

## Install

```sh
go get github.com/VinKurup/gotaskqueue
```

## Quick start (in-memory)

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/VinKurup/gotaskqueue"
)

func main() {
	q := gotaskqueue.NewMemoryQueue("emails")

	q.Register("send", func(ctx context.Context, t gotaskqueue.Task) error {
		fmt.Printf("sending to %s\n", t.Data)
		return nil
	})

	q.Start(4) // 4 background workers
	defer q.Stop()

	id, _ := q.Enqueue("send", []byte("hello@example.com"))

	time.Sleep(50 * time.Millisecond)
	task, _, _ := q.GetTask(id)
	fmt.Println(task.Status) // completed
}
```

## Redis backend

You supply the `*redis.Client` (so you control connection, auth, TLS, pool):

```go
import "github.com/redis/go-redis/v9"

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

q := gotaskqueue.NewRedisQueue(client, "emails",
	gotaskqueue.WithRedisMaxRetries(5),
	gotaskqueue.WithRedisVisibilityTimeout(30*time.Second),
)
q.Register("send", sendHandler)
q.Start(8)
defer q.Stop()

q.Enqueue("send", payload)
```

Producers and consumers can be separate processes/machines pointed at the same
Redis and queue name.

## The `Queue` interface

```go
type Queue interface {
	Enqueue(taskType string, data []byte) (id string, err error)
	EnqueueDelayed(taskType string, data []byte, delay time.Duration) (id string, err error)
	Register(taskType string, h Handler)
	Start(workers int)
	Stop()
	ProcessNext() (processed bool, err error) // run one job synchronously (no workers needed)
	GetTask(id string) (Task, bool, error)
	Cancel(id string) (bool, error)
	Stats() (Stats, error)
	Cleanup() (int, error)
	DeadLetters() ([]Task, error)
	Replay(id string) (bool, error)  // requeue a failed task
	Discard(id string) (bool, error) // drop a failed task
}

type Handler func(ctx context.Context, t Task) error
```

## Handler contract (read this)

**Handlers must be idempotent** — safe to run more than once for the same job.

With the Redis backend you get *at-least-once* delivery: if a worker dies after
doing its work but before acknowledging, the job is redelivered and your handler
runs again. This is not a bug; exactly-once *execution* is impossible across
process crashes. The standard pattern for effects that must happen once (charging
a card, etc.) is at-least-once delivery **plus** an idempotency key at the
business layer (e.g. a Stripe idempotency key) so the duplicate is harmless. The
queue provides the first half; the second half is your handler's job.

**Cancellation and timeouts are cooperative.** A handler is cancelled/timed out
by cancelling its `context`; it only actually stops if it observes `ctx.Done()`.
A handler that ignores its context can't be interrupted (Go can't kill a
goroutine) — respect the context in long operations.

## Task lifecycle

```
pending ─► processing ─► completed
   ▲            │
   │            ├─► (handler error, retries left) ─► scheduled ─► pending
scheduled ◄─────┘
   │            └─► (retries exhausted) ─► failed  (dead-letter: durable)
   └─ EnqueueDelayed / retry backoff

any queued/running ─► cancelled  (via Cancel)
```

`failed` is the dead-letter state: such tasks are exempt from cleanup and can be
inspected via `DeadLetters()`, requeued via `Replay(id)`, or dropped via
`Discard(id)`.

## Configuration

Common options (memory `WithX`, Redis `WithRedisX`):

| Option | Default | Meaning |
|---|---|---|
| `MaxRetries(n)` | 3 | Handler-error retries before dead-lettering |
| `Backoff(base, max)` | 100ms → 30s | Exponential backoff between retries |
| `TaskTTL(d)` | 1h | How long finished (completed/cancelled) tasks are kept |
| `CleanupInterval(d)` | 1m | Background cleanup cadence |
| `MaxTasks(n)` | 0 (off) | Cap on registry size; evicts oldest finished tasks |
| `HandlerTimeout(d)` | 0 (off) | Per-handler execution deadline |
| `ShutdownTimeout(d)` | 0 (wait) | Max time `Stop` waits for in-flight handlers |
| `Logger(l)` | none | Error logging sink (`*slog.Logger` works directly) |

Redis-only options:

| Option | Default | Meaning |
|---|---|---|
| `WithRedisVisibilityTimeout(d)` | 30s | How long a claimed job may run before the reaper redelivers it |
| `WithRedisMaxDeliveries(n)` | 3 | Crash-redelivery cap before poison → failed |
| `WithRedisPollInterval(d)` | 20ms | Promoter/reaper/cancel poll cadence |

## Delivery guarantees

- **MemoryQueue** — single process; everything is lost if the process exits.
- **RedisQueue** — **at-least-once**. Jobs are claimed atomically
  (`BRPOPLPUSH` into an in-flight list) with a visibility timeout; a background
  reaper redelivers jobs whose worker went silent. Enqueue, promotion, and
  reschedule are atomic (`MULTI`/Lua), so a job can't be registered-but-lost or
  dropped mid-promotion.

## Status & scope

The reliability core is complete and covered by tests (including a real-Redis
integration test, run with `REDIS_ADDR` set). Intentionally **not** included yet
(add per workload): priorities, per-job results/request-reply, cron/recurring
schedules, rate limiting, and a Redis Streams transport. See
`docs/production-readiness.md` for the full roadmap.

## Testing

```sh
go test ./...                                   # unit tests (uses miniredis, no server)
REDIS_ADDR=127.0.0.1:6379 go test -run Integration ./...   # against a real Redis
```
