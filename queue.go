package gotaskqueue

import (
	"sync"
)

const baseCapacity = 8

type Task struct {
	ID   string
	Type string
	Data []byte
}

type Queue struct {
	name  string
	mu    sync.Mutex
	items []Task 
	head  int    
	tail  int    
	count int    
}

func NewQueue(name string) *Queue {
	return &Queue{name: name}
}

func (q *Queue) Enqueue(t Task) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.count == len(q.items) {
		q.grow()
	}
	q.items[q.tail] = t
	q.tail = (q.tail + 1) % len(q.items)
	q.count++
}

func (q *Queue) Dequeue() (Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.count == 0 {
		return Task{}, false
	}
	t := q.items[q.head]
	q.items[q.head] = Task{} 
	q.head = (q.head + 1) % len(q.items)
	q.count--
	return t, true
}

func (q *Queue) grow() {
	newCap := len(q.items) * 2
	if newCap == 0 {
		newCap = baseCapacity
	}
	next := make([]Task, newCap)
	for i := 0; i < q.count; i++ {
		next[i] = q.items[(q.head+i)%len(q.items)]
	}
	q.items = next
	q.head = 0
	q.tail = q.count
}
