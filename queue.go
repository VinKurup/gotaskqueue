package gotaskqueue

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"
)

const baseCapacity = 8

const (
	defaultMaxRetries      = 3
	defaultBackoffBase     = 100 * time.Millisecond
	defaultBackoffMax      = 30 * time.Second
	defaultTaskTTL         = time.Hour
	defaultCleanupInterval = time.Minute
)

var ErrQueueStopped = errors.New("queue stopped")

type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusScheduled  TaskStatus = "scheduled"
	StatusProcessing TaskStatus = "processing"
	StatusCompleted  TaskStatus = "completed"
	StatusFailed     TaskStatus = "failed"
	StatusCancelled  TaskStatus = "cancelled"
)

type Task struct {
	ID         string
	Type       string
	Data       []byte
	Status     TaskStatus
	CreatedAt  time.Time
	FinishedAt time.Time
	Retries    int
	MaxRetries int
}

type Handler func(context.Context, Task) error

type Queue interface {
	Enqueue(taskType string, data []byte) (string, error)
	EnqueueDelayed(taskType string, data []byte, delay time.Duration) (string, error)
	Register(taskType string, h Handler)
	Start(workers int)
	Stop()
	ProcessNext() (bool, error)
	GetTask(id string) (Task, bool, error)
	Cancel(id string) (bool, error)
	Stats() (Stats, error)
	Cleanup() (int, error)
}

var (
	_ Queue = (*MemoryQueue)(nil)
	_ Queue = (*RedisQueue)(nil)
)

type Option func(*MemoryQueue)

func WithMaxRetries(n int) Option {
	return func(q *MemoryQueue) { q.maxRetries = n }
}

func WithBackoff(base, max time.Duration) Option {
	return func(q *MemoryQueue) {
		q.backoffBase = base
		q.backoffMax = max
	}
}

func WithTaskTTL(d time.Duration) Option {
	return func(q *MemoryQueue) { q.taskTTL = d }
}

func WithCleanupInterval(d time.Duration) Option {
	return func(q *MemoryQueue) { q.cleanupInterval = d }
}

type MemoryQueue struct {
	name            string
	mu              sync.Mutex
	cond            *sync.Cond
	items           []*Task
	head            int
	tail            int
	count           int
	delayed         delayHeap
	wake            chan struct{}
	tasks           map[string]*Task
	handlers        map[string]Handler
	inflight        map[string]context.CancelFunc
	idSeq           int64
	maxRetries      int
	backoffBase     time.Duration
	backoffMax      time.Duration
	taskTTL         time.Duration
	cleanupInterval time.Duration
	done            chan struct{}
	started         bool
	stopped         bool
	wg              sync.WaitGroup
}

func NewMemoryQueue(name string, opts ...Option) *MemoryQueue {
	q := &MemoryQueue{
		name:            name,
		wake:            make(chan struct{}, 1),
		tasks:           make(map[string]*Task),
		handlers:        make(map[string]Handler),
		inflight:        make(map[string]context.CancelFunc),
		maxRetries:      defaultMaxRetries,
		backoffBase:     defaultBackoffBase,
		backoffMax:      defaultBackoffMax,
		taskTTL:         defaultTaskTTL,
		cleanupInterval: defaultCleanupInterval,
		done:            make(chan struct{}),
	}
	q.cond = sync.NewCond(&q.mu)
	for _, opt := range opts {
		opt(q)
	}
	return q
}

func (q *MemoryQueue) Enqueue(taskType string, data []byte) (string, error) {
	if taskType == "" {
		return "", errors.New("task type cannot be empty")
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return "", ErrQueueStopped
	}

	t := q.buildTask(taskType, data, StatusPending)
	q.push(t)
	q.cond.Signal()
	return t.ID, nil
}

func (q *MemoryQueue) EnqueueDelayed(taskType string, data []byte, delay time.Duration) (string, error) {
	if taskType == "" {
		return "", errors.New("task type cannot be empty")
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return "", ErrQueueStopped
	}

	t := q.buildTask(taskType, data, StatusScheduled)
	q.delayed.push(&scheduledTask{task: t, runAt: time.Now().Add(delay)})
	q.wakeScheduler()
	return t.ID, nil
}

func (q *MemoryQueue) buildTask(taskType string, data []byte, status TaskStatus) *Task {
	q.idSeq++
	t := &Task{
		ID:         strconv.FormatInt(q.idSeq, 10),
		Type:       taskType,
		Data:       data,
		Status:     status,
		CreatedAt:  time.Now(),
		MaxRetries: q.maxRetries,
	}
	q.tasks[t.ID] = t
	return t
}

func (q *MemoryQueue) Register(taskType string, h Handler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[taskType] = h
}

func (q *MemoryQueue) Start(workers int) {
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
	go q.scheduler()
	q.wg.Add(1)
	go q.cleanupLoop()
}

func (q *MemoryQueue) Stop() {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.stopped = true
	q.cond.Broadcast()
	close(q.done)
	q.mu.Unlock()

	q.wakeScheduler()
	q.wg.Wait()
}

func (q *MemoryQueue) cleanupLoop() {
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

func (q *MemoryQueue) worker() {
	defer q.wg.Done()
	for {
		t := q.waitAndPop()
		if t == nil {
			return
		}
		q.runTask(t)
	}
}

func (q *MemoryQueue) waitAndPop() *Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		for q.count == 0 && !q.stopped {
			q.cond.Wait()
		}
		t := q.dequeue()
		if t == nil {
			return nil
		}
		if t.Status == StatusCancelled {
			continue
		}
		return t
	}
}

func (q *MemoryQueue) scheduler() {
	defer q.wg.Done()
	for {
		q.mu.Lock()
		if q.stopped {
			q.mu.Unlock()
			return
		}
		if q.promoteDue() > 0 {
			q.cond.Broadcast()
			q.mu.Unlock()
			continue
		}
		next := q.delayed.peek()
		q.mu.Unlock()

		if next == nil {
			<-q.wake
			continue
		}
		timer := time.NewTimer(time.Until(next.runAt))
		select {
		case <-timer.C:
		case <-q.wake:
			timer.Stop()
		}
	}
}

func (q *MemoryQueue) promoteDue() int {
	now := time.Now()
	n := 0
	for st := q.delayed.peek(); st != nil && !st.runAt.After(now); st = q.delayed.peek() {
		q.delayed.pop()
		if st.task.Status == StatusCancelled {
			continue
		}
		st.task.Status = StatusPending
		q.push(st.task)
		n++
	}
	return n
}

func (q *MemoryQueue) wakeScheduler() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *MemoryQueue) ProcessNext() (processed bool, err error) {
	q.mu.Lock()
	q.promoteDue()
	var t *Task
	for {
		t = q.dequeue()
		if t == nil || t.Status != StatusCancelled {
			break
		}
	}
	q.mu.Unlock()
	if t == nil {
		return false, nil
	}
	return true, q.runTask(t)
}

func (q *MemoryQueue) runTask(t *Task) error {
	q.mu.Lock()
	t.Status = StatusProcessing
	h, ok := q.handlers[t.Type]
	if !ok {
		q.mu.Unlock()
		q.setStatus(t, StatusFailed)
		return errors.New("no handler registered for task type " + t.Type)
	}
	ctx, cancel := context.WithCancel(context.Background())
	q.inflight[t.ID] = cancel
	q.mu.Unlock()

	err := h(ctx, *t)

	q.mu.Lock()
	delete(q.inflight, t.ID)
	cancelled := ctx.Err() != nil
	cancel()
	if cancelled {
		t.Status = StatusCancelled
		t.FinishedAt = time.Now()
		q.mu.Unlock()
		return err
	}
	q.mu.Unlock()

	if err != nil {
		q.retryOrFail(t)
		return err
	}
	q.setStatus(t, StatusCompleted)
	return nil
}

func (q *MemoryQueue) retryOrFail(t *Task) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if t.Retries < t.MaxRetries {
		t.Retries++
		t.Status = StatusScheduled
		q.delayed.push(&scheduledTask{task: t, runAt: time.Now().Add(q.backoff(t.Retries))})
		q.wakeScheduler()
		return
	}
	t.Status = StatusFailed
	t.FinishedAt = time.Now()
}

func (q *MemoryQueue) backoff(retries int) time.Duration {
	return expBackoff(q.backoffBase, q.backoffMax, retries)
}

func expBackoff(base, max time.Duration, retries int) time.Duration {
	d := base << (retries - 1)
	if d <= 0 || d > max {
		d = max
	}
	return d
}

func (q *MemoryQueue) GetTask(id string) (Task, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return Task{}, false, nil
	}
	return *t, true, nil
}

func (q *MemoryQueue) setStatus(t *Task, s TaskStatus) {
	q.mu.Lock()
	t.Status = s
	if s == StatusCompleted || s == StatusFailed {
		t.FinishedAt = time.Now()
	}
	q.mu.Unlock()
}

func (q *MemoryQueue) Cancel(id string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return false, nil
	}
	switch t.Status {
	case StatusPending, StatusScheduled:
		t.Status = StatusCancelled
		t.FinishedAt = time.Now()
		return true, nil
	case StatusProcessing:
		if cancel, ok := q.inflight[t.ID]; ok {
			cancel()
			return true, nil
		}
	}
	return false, nil
}

func (q *MemoryQueue) Cleanup() (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.purgeExpired(time.Now()), nil
}

func (q *MemoryQueue) purgeExpired(now time.Time) int {
	n := 0
	for id, t := range q.tasks {
		if isTerminal(t.Status) && now.Sub(t.FinishedAt) > q.taskTTL {
			delete(q.tasks, id)
			n++
		}
	}
	return n
}

func isTerminal(s TaskStatus) bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCancelled
}

type Stats struct {
	Pending    int
	Scheduled  int
	Processing int
	Completed  int
	Failed     int
	Cancelled  int
	Total      int
}

func (q *MemoryQueue) Stats() (Stats, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var s Stats
	for _, t := range q.tasks {
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

func (q *MemoryQueue) push(t *Task) {
	if q.count == len(q.items) {
		q.grow()
	}
	q.items[q.tail] = t
	q.tail = (q.tail + 1) % len(q.items)
	q.count++
}

func (q *MemoryQueue) dequeue() *Task {
	if q.count == 0 {
		return nil
	}
	t := q.items[q.head]
	q.items[q.head] = nil
	q.head = (q.head + 1) % len(q.items)
	q.count--
	return t
}

func (q *MemoryQueue) grow() {
	newCap := len(q.items) * 2
	if newCap == 0 {
		newCap = baseCapacity
	}
	next := make([]*Task, newCap)
	for i := 0; i < q.count; i++ {
		next[i] = q.items[(q.head+i)%len(q.items)]
	}
	q.items = next
	q.head = 0
	q.tail = q.count
}

type scheduledTask struct {
	task  *Task
	runAt time.Time
}

type delayHeap struct {
	items []*scheduledTask
}

func (h *delayHeap) peek() *scheduledTask {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

func (h *delayHeap) push(st *scheduledTask) {
	h.items = append(h.items, st)
	h.siftUp(len(h.items) - 1)
}

func (h *delayHeap) pop() *scheduledTask {
	n := len(h.items)
	if n == 0 {
		return nil
	}
	root := h.items[0]
	last := h.items[n-1]
	h.items[n-1] = nil
	h.items = h.items[:n-1]
	if len(h.items) > 0 {
		h.items[0] = last
		h.siftDown(0)
	}
	return root
}

func (h *delayHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.items[i].runAt.Before(h.items[parent].runAt) {
			break
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

func (h *delayHeap) siftDown(i int) {
	n := len(h.items)
	for {
		smallest := i
		if l := 2*i + 1; l < n && h.items[l].runAt.Before(h.items[smallest].runAt) {
			smallest = l
		}
		if r := 2*i + 2; r < n && h.items[r].runAt.Before(h.items[smallest].runAt) {
			smallest = r
		}
		if smallest == i {
			break
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
}
