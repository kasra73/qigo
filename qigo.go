/**
 * Author: Seyed Farzam Mirmoeini - mirfarzam
 * File: qigo.go
 */

package qigo

import "time"

type QueueHandler[T any] struct {
	queueChannel chan T
	Queue        []T
	fn           func([]T)
}

func (e *QueueHandler[T]) Init(fn func([]T)) {
	e.queueChannel = make(chan T)
	e.Queue = make([]T, 0)
	e.fn = fn
}

func (h *QueueHandler[T]) Append(p T) {
	h.queueChannel <- p
}

func (h *QueueHandler[T]) AppendBatch(ps []T) {
	for _, p := range ps {
		h.queueChannel <- p
	}
}

func (h *QueueHandler[T]) callFunction() {
	if len(h.Queue) == 0 {
		return
	}
	h.fn(h.Queue)
	h.Queue = nil
}

func (h *QueueHandler[T]) Start(maxTimeMS int, maxSize int) error {
	go func() {
		for {
			h.Queue = append(h.Queue, <-h.queueChannel)
			if maxSize <= len(h.Queue) {
				h.callFunction()
			}
		}
	}()
	ticker := time.NewTicker(time.Duration(maxTimeMS) * time.Millisecond)
	go func() {
		for {
			select {
			case _ = <-ticker.C:
				h.callFunction()
			}
		}
	}()
	return nil
}
