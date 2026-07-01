package gotaskqueue

import (
	"errors"
	"strconv"
	"sync"
	"time"
)

const baseCapacity = 8

var ErrQueueStopped = errors.New("queue stopped")

type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusProcessing TaskStatus = "processing"
	StatusCompleted  TaskStatus = "completed"
	StatusFailed     TaskStatus = "failed"
)

type Task struct {
	ID        string
	Type      string
	Data      []byte
	Status    TaskStatus
	CreatedAt time.Time
}

type Handler func(Task) error

type Queue struct {
	name string
	mu   sync.Mutex
	cond *sync.Cond 

	items []*Task
	head  int
	tail  int
	count int

	tasks    map[string]*Task   
	handlers map[string]Handler 
	idSeq    int64              

	started bool
	stopped bool
	wg      sync.WaitGroup 
}

func NewQueue(name string) *Queue {
	q := &Queue{
		name:     name,
		tasks:    make(map[string]*Task),
		handlers: make(map[string]Handler),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Enqueue(taskType string, data []byte) (string, error) {
	if taskType == "" {
		return "", errors.New("task type cannot be empty")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.stopped {
		return "", ErrQueueStopped
	}

	q.idSeq++
	t := &Task{
		ID:        strconv.FormatInt(q.idSeq, 10),
		Type:      taskType,
		Data:      data,
		Status:    StatusPending,
		CreatedAt: time.Now(),
	}
	q.tasks[t.ID] = t
	q.push(t)
	q.cond.Signal() 
	return t.ID, nil
}


func (q *Queue) Register(taskType string, h Handler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[taskType] = h
}

func (q *Queue) Start(workers int) {
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
}

func (q *Queue) Stop() {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.stopped = true
	q.cond.Broadcast()
	q.mu.Unlock()

	q.wg.Wait()
}


func (q *Queue) worker() {
	defer q.wg.Done()
	for {
		t := q.waitAndPop()
		if t == nil {
			return
		}
		q.runTask(t)
	}
}

func (q *Queue) waitAndPop() *Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.count == 0 && !q.stopped {
		q.cond.Wait()
	}
	return q.dequeue() 
}

func (q *Queue) ProcessNext() (processed bool, err error) {
	q.mu.Lock()
	t := q.dequeue()
	q.mu.Unlock()
	if t == nil {
		return false, nil
	}
	return true, q.runTask(t)
}

func (q *Queue) runTask(t *Task) error {
	q.mu.Lock()
	t.Status = StatusProcessing
	h, ok := q.handlers[t.Type]
	q.mu.Unlock()

	if !ok {
		q.setStatus(t, StatusFailed)
		return errors.New("no handler registered for task type " + t.Type)
	}
	if err := h(*t); err != nil {
		q.setStatus(t, StatusFailed)
		return err
	}
	q.setStatus(t, StatusCompleted)
	return nil
}

func (q *Queue) GetTask(id string) (Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return Task{}, false
	}
	return *t, true
}

func (q *Queue) setStatus(t *Task, s TaskStatus) {
	q.mu.Lock()
	t.Status = s
	q.mu.Unlock()
}

func (q *Queue) push(t *Task) {
	if q.count == len(q.items) {
		q.grow()
	}
	q.items[q.tail] = t
	q.tail = (q.tail + 1) % len(q.items)
	q.count++
}

func (q *Queue) dequeue() *Task {
	if q.count == 0 {
		return nil
	}
	t := q.items[q.head]
	q.items[q.head] = nil
	q.head = (q.head + 1) % len(q.items)
	q.count--
	return t
}

func (q *Queue) grow() {
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
