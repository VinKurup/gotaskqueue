package gotaskqueue

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultRedisPoll = 20 * time.Millisecond

const cancelSignalTTL = 5 * time.Minute

const brpopTimeout = time.Second

const stopSentinel = "__stop__"

const defaultVisibilityTimeout = 30 * time.Second

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

func WithRedisMaxTasks(n int) RedisOption {
	return func(q *RedisQueue) { q.maxTasks = n }
}

func WithRedisVisibilityTimeout(d time.Duration) RedisOption {
	return func(q *RedisQueue) { q.visibilityTimeout = d }
}

type RedisQueue struct {
	client            *redis.Client
	name              string
	pollInterval      time.Duration
	maxRetries        int
	backoffBase       time.Duration
	backoffMax        time.Duration
	taskTTL           time.Duration
	cleanupInterval   time.Duration
	maxTasks          int
	visibilityTimeout time.Duration

	mu       sync.Mutex
	handlers map[string]Handler
	done     chan struct{}
	workers  int
	started  bool
	stopped  bool
	wg       sync.WaitGroup
}

func NewRedisQueue(client *redis.Client, name string, opts ...RedisOption) *RedisQueue {
	q := &RedisQueue{
		client:            client,
		name:              name,
		pollInterval:      defaultRedisPoll,
		maxRetries:        defaultMaxRetries,
		backoffBase:       defaultBackoffBase,
		backoffMax:        defaultBackoffMax,
		taskTTL:           defaultTaskTTL,
		cleanupInterval:   defaultCleanupInterval,
		visibilityTimeout: defaultVisibilityTimeout,
		handlers:          make(map[string]Handler),
		done:              make(chan struct{}),
	}
	for _, o := range opts {
		o(q)
	}
	return q
}

func (q *RedisQueue) readyKey() string           { return q.name + ":ready" }
func (q *RedisQueue) tasksKey() string           { return q.name + ":tasks" }
func (q *RedisQueue) delayedKey() string         { return q.name + ":delayed" }
func (q *RedisQueue) seqKey() string             { return q.name + ":seq" }
func (q *RedisQueue) inflightKey() string        { return q.name + ":inflight" }
func (q *RedisQueue) deadlineKey() string        { return q.name + ":deadlines" }
func (q *RedisQueue) cancelKey(id string) string { return q.name + ":cancel:" + id }

func (q *RedisQueue) Enqueue(taskType string, data []byte) (string, error) {
	ctx := context.Background()
	t, b, err := q.newTask(ctx, taskType, data, StatusPending)
	if err != nil {
		return "", err
	}
	// Register the task and make it runnable atomically, so a failure can't
	// leave a task in the registry that is in no ready list.
	_, err = q.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, q.tasksKey(), t.ID, b)
		p.LPush(ctx, q.readyKey(), t.ID)
		return nil
	})
	if err != nil {
		return "", err
	}
	return t.ID, nil
}

func (q *RedisQueue) EnqueueDelayed(taskType string, data []byte, delay time.Duration) (string, error) {
	ctx := context.Background()
	t, b, err := q.newTask(ctx, taskType, data, StatusScheduled)
	if err != nil {
		return "", err
	}
	_, err = q.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, q.tasksKey(), t.ID, b)
		p.ZAdd(ctx, q.delayedKey(), redis.Z{Score: unixScore(time.Now().Add(delay)), Member: t.ID})
		return nil
	})
	if err != nil {
		return "", err
	}
	return t.ID, nil
}

// newTask allocates an ID and builds a task with its marshaled form. It does not
// write anything; the caller registers and enqueues it atomically.
func (q *RedisQueue) newTask(ctx context.Context, taskType string, data []byte, status TaskStatus) (*Task, []byte, error) {
	if taskType == "" {
		return nil, nil, errors.New("task type cannot be empty")
	}
	n, err := q.client.Incr(ctx, q.seqKey()).Result()
	if err != nil {
		return nil, nil, err
	}
	t := &Task{
		ID:         strconv.FormatInt(n, 10),
		Type:       taskType,
		Data:       data,
		Status:     status,
		CreatedAt:  time.Now(),
		MaxRetries: q.maxRetries,
	}
	b, err := json.Marshal(t)
	if err != nil {
		return nil, nil, err
	}
	return t, b, nil
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
	q.workers = workers
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	q.wg.Add(1)
	go q.promoter()
	q.wg.Add(1)
	go q.cleanupLoop()
	q.wg.Add(1)
	go q.reaper()
}

func (q *RedisQueue) Stop() {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.stopped = true
	close(q.done)
	n := q.workers
	q.mu.Unlock()

	if n > 0 {
		sentinels := make([]interface{}, n)
		for i := range sentinels {
			sentinels[i] = stopSentinel
		}
		q.client.RPush(context.Background(), q.readyKey(), sentinels...)
	}
	q.wg.Wait()
}

func (q *RedisQueue) worker() {
	defer q.wg.Done()
	ctx := context.Background()
	for {
		id, err := q.client.BRPopLPush(ctx, q.readyKey(), q.inflightKey(), brpopTimeout).Result()
		if err != nil {
			if err == redis.Nil {
				select {
				case <-q.done:
					return
				default:
					continue
				}
			}
			select {
			case <-q.done:
				return
			case <-time.After(q.pollInterval):
			}
			continue
		}
		if id == stopSentinel {
			q.client.LRem(ctx, q.inflightKey(), 1, stopSentinel)
			return
		}

		q.extendDeadline(ctx, id)
		t, ok, err := q.getTask(ctx, id)
		if err != nil || !ok || t.Status == StatusCancelled {
			q.ackInflight(ctx, id)
			continue
		}

		hbDone := make(chan struct{})
		go q.heartbeat(id, hbDone)
		q.runTask(ctx, &t)
		close(hbDone)
		q.ackInflight(ctx, id)
	}
}

// ackInflight removes a finished job from the in-flight tracking so the reaper
// won't redeliver it.
func (q *RedisQueue) ackInflight(ctx context.Context, id string) {
	q.client.LRem(ctx, q.inflightKey(), 1, id)
	q.client.ZRem(ctx, q.deadlineKey(), id)
}

func (q *RedisQueue) extendDeadline(ctx context.Context, id string) {
	q.client.ZAdd(ctx, q.deadlineKey(), redis.Z{
		Score:  unixScore(time.Now().Add(q.visibilityTimeout)),
		Member: id,
	})
}

// heartbeat keeps a running job's deadline in the future so a slow-but-alive
// worker is not mistaken for a dead one.
func (q *RedisQueue) heartbeat(id string, done chan struct{}) {
	interval := q.visibilityTimeout / 3
	if interval <= 0 {
		interval = q.pollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	ctx := context.Background()
	for {
		select {
		case <-done:
			return
		case <-q.done:
			return
		case <-ticker.C:
			q.extendDeadline(ctx, id)
		}
	}
}

func (q *RedisQueue) reconcileInflight(ctx context.Context) {
	ids, err := q.client.LRange(ctx, q.inflightKey(), 0, -1).Result()
	if err != nil {
		return
	}
	for _, id := range ids {
		if id == stopSentinel {
			continue
		}
		if _, err := q.client.ZScore(ctx, q.deadlineKey(), id).Result(); err == redis.Nil {
			q.extendDeadline(ctx, id)
		}
	}
}

func (q *RedisQueue) reaper() {
	defer q.wg.Done()
	ctx := context.Background()
	for {
		q.reap(ctx)
		select {
		case <-q.done:
			return
		case <-time.After(q.pollInterval):
		}
	}
}

func (q *RedisQueue) reap(ctx context.Context) {
	q.reconcileInflight(ctx)

	now := strconv.FormatFloat(unixScore(time.Now()), 'f', -1, 64)
	ids, err := q.client.ZRangeByScore(ctx, q.deadlineKey(), &redis.ZRangeBy{Min: "0", Max: now}).Result()
	if err != nil {
		return
	}
	for _, id := range ids {
		claimed, err := q.client.ZRem(ctx, q.deadlineKey(), id).Result()
		if err != nil || claimed == 0 {
			continue // another reaper claimed it
		}
		q.client.LRem(ctx, q.inflightKey(), 1, id)

		t, ok, err := q.getTask(ctx, id)
		if err != nil || !ok || isTerminal(t.Status) {
			continue // finished; ack was just lost, nothing to redeliver
		}

		t.Retries++
		if t.Retries > t.MaxRetries {
			t.Status = StatusFailed
			t.FinishedAt = time.Now()
			q.save(ctx, &t)
			continue
		}
		t.Status = StatusPending
		q.save(ctx, &t)
		q.client.LPush(ctx, q.readyKey(), id)
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

	q.client.Del(ctx, q.cancelKey(t.ID))
	hctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan struct{})
	go q.watchCancel(t.ID, cancel, watchDone)

	t.Status = StatusProcessing
	if err := q.save(ctx, t); err != nil {
		close(watchDone)
		return err
	}

	herr := h(hctx, *t)
	close(watchDone)
	q.client.Del(ctx, q.cancelKey(t.ID))

	if hctx.Err() != nil {
		t.Status = StatusCancelled
		t.FinishedAt = time.Now()
		_ = q.save(ctx, t)
		return herr
	}
	if herr != nil {
		return q.retryOrFail(ctx, t, herr)
	}
	t.Status = StatusCompleted
	t.FinishedAt = time.Now()
	return q.save(ctx, t)
}

func (q *RedisQueue) watchCancel(id string, cancel context.CancelFunc, done chan struct{}) {
	ticker := time.NewTicker(q.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-q.done:
			return
		case <-ticker.C:
			if n, err := q.client.Exists(context.Background(), q.cancelKey(id)).Result(); err == nil && n > 0 {
				cancel()
				return
			}
		}
	}
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
	var status TaskStatus
	txf := func(tx *redis.Tx) error {
		status = ""
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
		status = t.Status
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
		return err
	}
	committed := false
	for i := 0; i < 3; i++ {
		err := q.client.Watch(ctx, txf, q.tasksKey())
		if err == redis.TxFailedErr {
			continue
		}
		if err != nil {
			return false, err
		}
		committed = true
		break
	}
	if !committed {
		return false, redis.TxFailedErr
	}

	switch status {
	case StatusPending, StatusScheduled, StatusProcessing:
		if err := q.client.Set(ctx, q.cancelKey(id), "1", cancelSignalTTL).Err(); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
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

	remaining := 0
	type finished struct {
		id string
		at time.Time
	}
	var terminal []finished
	for id, v := range m {
		var t Task
		if err := json.Unmarshal([]byte(v), &t); err != nil {
			continue
		}
		if isTerminal(t.Status) && now.Sub(t.FinishedAt) > q.taskTTL {
			if q.client.HDel(ctx, q.tasksKey(), id).Err() == nil {
				n++
			}
			continue
		}
		remaining++
		if isTerminal(t.Status) {
			terminal = append(terminal, finished{id, t.FinishedAt})
		}
	}

	if q.maxTasks > 0 && remaining > q.maxTasks {
		sort.Slice(terminal, func(i, j int) bool {
			return terminal[i].at.Before(terminal[j].at)
		})
		excess := remaining - q.maxTasks
		for _, f := range terminal {
			if excess <= 0 {
				break
			}
			if q.client.HDel(ctx, q.tasksKey(), f.id).Err() == nil {
				n++
				excess--
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
