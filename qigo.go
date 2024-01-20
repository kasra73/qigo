/**
 * Author: Seyed Farzam Mirmoeini - mirfarzam
 * File: qigo.go
 */

package qigo

import (
	"log"
	"sync"
	"time"
)

type QueueHandler[T any] struct {
	queueChannel chan T
	Queue        []T
	fn           func([]T)
	mu           sync.Mutex
}

func (e *QueueHandler[T]) Init(fn func([]T)) {
	e.queueChannel = make(chan T)
	e.Queue = make([]T, 0)
	e.fn = fn
}

func (h *QueueHandler[T]) Append(p T) {
	println("appended -> ", p)
	h.queueChannel <- p
}

func (h *QueueHandler[T]) AppendBatch(ps []T) {
	for _, p := range ps {
		h.queueChannel <- p
	}
}

func (h *QueueHandler[T]) callFunction() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.Queue) == 0 {
		return
	}
	h.fn(h.Queue)
	h.Queue = make([]T, 0)
}

func (h *QueueHandler[T]) Start(maxTimeMS int, maxSize int) error {
	duration := time.Duration(maxTimeMS) * time.Millisecond
	go func() {
		for {
			select {
			case p := <-h.queueChannel:
				h.mu.Lock()
				h.Queue = append(h.Queue, p)
				h.mu.Unlock()
				if maxSize <= len(h.Queue) {
					log.Println("Processing by max batch size ...")
					h.callFunction()
				}
			case <-time.After(duration):
				log.Println("Processing by ticker ...")
				h.callFunction()
			}
		}
	}()
	return nil
}
