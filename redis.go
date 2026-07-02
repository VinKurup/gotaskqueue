package gotaskqueue

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultRedisPoll = 20 * time.Millisecond

type RedisOption func(*RedisQueue)

func WithRedisMaxRetries(n int) RedisOption {
	return func(q *RedisQueue) { q.maxRetries = n }
}

func WithRedisBackoff(base, max time.Duration) RedisOption {
	return func(q *RedisQueue) {
		q.backoffBase = base
		q.backoffMax = max
	}
}

func WithRedisPollInterval(d time.Duration) RedisOption {
	return func(q *RedisQueue) { q.pollInterval = d }
}

func WithRedisTaskTTL(d time.Duration) RedisOption {
	return func(q *RedisQueue) { q.taskTTL = d }
}

func WithRedisCleanupInterval(d time.Duration) RedisOption {
	return func(q *RedisQueue) { q.cleanupInterval = d }
}

type RedisQueue struct {
	client          *redis.Client
	name            string
	pollInterval    time.Duration
	maxRetries      int
	backoffBase     time.Duration
	backoffMax      time.Duration
	taskTTL         time.Duration
	cleanupInterval time.Duration

	mu       sync.Mutex
	handlers map[string]Handler
	done     chan struct{}
	started  bool
	stopped  bool
	wg       sync.WaitGroup
}

func NewRedisQueue(client *redis.Client, name string, opts ...RedisOption) *RedisQueue {
	q := &RedisQueue{
		client:          client,
		name:            name,
		pollInterval:    defaultRedisPoll,
		maxRetries:      defaultMaxRetries,
		backoffBase:     defaultBackoffBase,
		backoffMax:      defaultBackoffMax,
		taskTTL:         defaultTaskTTL,
		cleanupInterval: defaultCleanupInterval,
		handlers:        make(map[string]Handler),
		done:            make(chan struct{}),
	}
	for _, o := range opts {
		o(q)
	}
	return q
}

func (q *RedisQueue) readyKey() string   { return q.name + ":ready" }
func (q *RedisQueue) tasksKey() string   { return q.name + ":tasks" }
func (q *RedisQueue) delayedKey() string { return q.name + ":delayed" }
func (q *RedisQueue) seqKey() string     { return q.name + ":seq" }

func (q *RedisQueue) Enqueue(taskType string, data []byte) (string, error) {
	t, err := q.newTask(context.Background(), taskType, data, StatusPending)
	if err != nil {
		return "", err
	}
	if err := q.client.LPush(context.Background(), q.readyKey(), t.ID).Err(); err != nil {
		return "", err
	}
	return t.ID, nil
}

func (q *RedisQueue) EnqueueDelayed(taskType string, data []byte, delay time.Duration) (string, error) {
	ctx := context.Background()
	t, err := q.newTask(ctx, taskType, data, StatusScheduled)
	if err != nil {
		return "", err
	}
	if err := q.schedule(ctx, t.ID, time.Now().Add(delay)); err != nil {
		return "", err
	}
	return t.ID, nil
}

func (q *RedisQueue) newTask(ctx context.Context, taskType string, data []byte, status TaskStatus) (*Task, error) {
	if taskType == "" {
		return nil, errors.New("task type cannot be empty")
	}
	n, err := q.client.Incr(ctx, q.seqKey()).Result()
	if err != nil {
		return nil, err
	}
	t := &Task{
		ID:         strconv.FormatInt(n, 10),
		Type:       taskType,
		Data:       data,
		Status:     status,
		CreatedAt:  time.Now(),
		MaxRetries: q.maxRetries,
	}
	if err := q.save(ctx, t); err != nil {
		return nil, err
	}
	return t, nil
}

func (q *RedisQueue) Register(taskType string, h Handler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[taskType] = h
}

func (q *RedisQueue) GetTask(id string) (Task, bool, error) {
	return q.getTask(context.Background(), id)
}

func (q *RedisQueue) ProcessNext() (bool, error) {
	ctx := context.Background()
	q.promoteDue(ctx)
	for {
		id, err := q.client.RPop(ctx, q.readyKey()).Result()
		if err == redis.Nil {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		t, ok, err := q.getTask(ctx, id)
		if err != nil {
			return false, err
		}
		if !ok || t.Status == StatusCancelled {
			continue
		}
		return true, q.runTask(ctx, &t)
	}
}

func (q *RedisQueue) Start(workers int) {
	if workers < 1 {
		workers = 1
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started || q.stopped {
		return
	}
	q.started = true
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	q.wg.Add(1)
	go q.promoter()
	q.wg.Add(1)
	go q.cleanupLoop()
}

func (q *RedisQueue) Stop() {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.stopped = true
	close(q.done)
	q.mu.Unlock()
	q.wg.Wait()
}

func (q *RedisQueue) worker() {
	defer q.wg.Done()
	ctx := context.Background()
	for {
		select {
		case <-q.done:
			return
		default:
		}
		id, err := q.client.RPop(ctx, q.readyKey()).Result()
		if err != nil {
			select {
			case <-q.done:
				return
			case <-time.After(q.pollInterval):
			}
			continue
		}
		t, ok, err := q.getTask(ctx, id)
		if err != nil || !ok || t.Status == StatusCancelled {
			continue
		}
		q.runTask(ctx, &t)
	}
}

func (q *RedisQueue) promoter() {
	defer q.wg.Done()
	ctx := context.Background()
	for {
		q.promoteDue(ctx)
		select {
		case <-q.done:
			return
		case <-time.After(q.pollInterval):
		}
	}
}

func (q *RedisQueue) promoteDue(ctx context.Context) {
	now := strconv.FormatFloat(unixScore(time.Now()), 'f', -1, 64)
	ids, err := q.client.ZRangeByScore(ctx, q.delayedKey(), &redis.ZRangeBy{Min: "0", Max: now}).Result()
	if err != nil {
		return
	}
	for _, id := range ids {
		claimed, err := q.client.ZRem(ctx, q.delayedKey(), id).Result()
		if err != nil || claimed == 0 {
			continue
		}
		t, ok, err := q.getTask(ctx, id)
		if err != nil || !ok || t.Status == StatusCancelled {
			continue
		}
		t.Status = StatusPending
		q.save(ctx, &t)
		q.client.LPush(ctx, q.readyKey(), id)
	}
}

func (q *RedisQueue) runTask(ctx context.Context, t *Task) error {
	q.mu.Lock()
	h, ok := q.handlers[t.Type]
	q.mu.Unlock()

	if !ok {
		t.Status = StatusFailed
		t.FinishedAt = time.Now()
		_ = q.save(ctx, t)
		return errors.New("no handler registered for task type " + t.Type)
	}

	t.Status = StatusProcessing
	if err := q.save(ctx, t); err != nil {
		return err
	}

	herr := h(ctx, *t)
	if herr != nil {
		return q.retryOrFail(ctx, t, herr)
	}
	t.Status = StatusCompleted
	t.FinishedAt = time.Now()
	return q.save(ctx, t)
}

func (q *RedisQueue) retryOrFail(ctx context.Context, t *Task, herr error) error {
	if t.Retries < t.MaxRetries {
		t.Retries++
		t.Status = StatusScheduled
		if err := q.save(ctx, t); err != nil {
			return err
		}
		q.schedule(ctx, t.ID, time.Now().Add(expBackoff(q.backoffBase, q.backoffMax, t.Retries)))
		return herr
	}
	t.Status = StatusFailed
	t.FinishedAt = time.Now()
	_ = q.save(ctx, t)
	return herr
}

func (q *RedisQueue) schedule(ctx context.Context, id string, runAt time.Time) error {
	return q.client.ZAdd(ctx, q.delayedKey(), redis.Z{Score: unixScore(runAt), Member: id}).Err()
}

func (q *RedisQueue) Cancel(id string) (bool, error) {
	ctx := context.Background()
	cancelled := false
	txf := func(tx *redis.Tx) error {
		cancelled = false
		b, err := tx.HGet(ctx, q.tasksKey(), id).Bytes()
		if err == redis.Nil {
			return nil
		}
		if err != nil {
			return err
		}
		var t Task
		if err := json.Unmarshal(b, &t); err != nil {
			return err
		}
		if t.Status != StatusPending && t.Status != StatusScheduled {
			return nil
		}
		t.Status = StatusCancelled
		t.FinishedAt = time.Now()
		nb, err := json.Marshal(&t)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.HSet(ctx, q.tasksKey(), id, nb)
			return nil
		})
		if err == nil {
			cancelled = true
		}
		return err
	}
	for i := 0; i < 3; i++ {
		err := q.client.Watch(ctx, txf, q.tasksKey())
		if err == redis.TxFailedErr {
			continue
		}
		return cancelled, err
	}
	return false, redis.TxFailedErr
}

func (q *RedisQueue) Stats() (Stats, error) {
	m, err := q.client.HGetAll(context.Background(), q.tasksKey()).Result()
	if err != nil {
		return Stats{}, err
	}
	var s Stats
	for _, v := range m {
		var t Task
		if err := json.Unmarshal([]byte(v), &t); err != nil {
			continue
		}
		s.Total++
		switch t.Status {
		case StatusPending:
			s.Pending++
		case StatusScheduled:
			s.Scheduled++
		case StatusProcessing:
			s.Processing++
		case StatusCompleted:
			s.Completed++
		case StatusFailed:
			s.Failed++
		case StatusCancelled:
			s.Cancelled++
		}
	}
	return s, nil
}

func (q *RedisQueue) Cleanup() (int, error) {
	ctx := context.Background()
	m, err := q.client.HGetAll(ctx, q.tasksKey()).Result()
	if err != nil {
		return 0, err
	}
	now := time.Now()
	n := 0
	for id, v := range m {
		var t Task
		if err := json.Unmarshal([]byte(v), &t); err != nil {
			continue
		}
		if isTerminal(t.Status) && now.Sub(t.FinishedAt) > q.taskTTL {
			if q.client.HDel(ctx, q.tasksKey(), id).Err() == nil {
				n++
			}
		}
	}
	return n, nil
}

func (q *RedisQueue) cleanupLoop() {
	defer q.wg.Done()
	ticker := time.NewTicker(q.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			q.Cleanup()
		case <-q.done:
			return
		}
	}
}

func (q *RedisQueue) getTask(ctx context.Context, id string) (Task, bool, error) {
	b, err := q.client.HGet(ctx, q.tasksKey(), id).Bytes()
	if err == redis.Nil {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, err
	}
	var t Task
	if err := json.Unmarshal(b, &t); err != nil {
		return Task{}, false, err
	}
	return t, true, nil
}

func (q *RedisQueue) save(ctx context.Context, t *Task) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return q.client.HSet(ctx, q.tasksKey(), t.ID, b).Err()
}

func unixScore(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}
