package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type subscriber struct {
	ch      chan string
	pending bool
}

type topicQueue struct {
	records     []string
	subscribers []*subscriber
}

type broker struct {
	mu     sync.Mutex
	topics map[string]*topicQueue
}

func newBroker() *broker {
	return &broker{topics: make(map[string]*topicQueue)}
}

func (b *broker) getTopicQueue(topicName string) *topicQueue {
	if b.topics[topicName] == nil {
		b.topics[topicName] = &topicQueue{}
	}
	return b.topics[topicName]
}

func (b *broker) produce(topicName, record string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic := b.getTopicQueue(topicName)
	for len(topic.subscribers) > 0 {
		sub := topic.subscribers[0]
		topic.subscribers = topic.subscribers[1:]
		if !sub.pending {
			continue
		}
		sub.pending = false
		sub.ch <- record
		return
	}

	topic.records = append(topic.records, record)
}

func (b *broker) cancelSubscriber(topicName string, sub *subscriber) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !sub.pending {
		return false
	}
	sub.pending = false
	topic := b.getTopicQueue(topicName)
	for i := range topic.subscribers {
		if topic.subscribers[i] == sub {
			topic.subscribers = append(topic.subscribers[:i], topic.subscribers[i+1:]...)
			break
		}
	}
	return true
}

func (b *broker) consume(ctx context.Context, topicName string, timeout time.Duration) (string, bool) {
	b.mu.Lock()

	topic := b.getTopicQueue(topicName)
	if len(topic.records) > 0 {
		record := topic.records[0]
		topic.records = topic.records[1:]
		b.mu.Unlock()
		return record, true
	}

	if timeout == 0 {
		b.mu.Unlock()
		return "", false
	}

	ch := make(chan string, 1)
	sub := &subscriber{ch: ch, pending: true}
	topic.subscribers = append(topic.subscribers, sub)
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case record := <-ch:
		return record, true
	case <-timer.C:
		if b.cancelSubscriber(topicName, sub) {
			return "", false
		}
		return <-ch, true
	case <-ctx.Done():
		if b.cancelSubscriber(topicName, sub) {
			return "", false
		}
		return <-ch, true
	}
}

func (b *broker) handleProduce(w http.ResponseWriter, r *http.Request) {
	record := r.URL.Query().Get("v")
	if record == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	topicName := r.URL.Path[1:]
	b.produce(topicName, record)
	w.WriteHeader(http.StatusOK)
}

func (b *broker) handleConsume(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Path[1:]
	timeoutStr := r.URL.Query().Get("timeout")

	var timeout time.Duration
	if timeoutStr != "" {
		seconds, err := strconv.Atoi(timeoutStr)
		if err != nil || seconds <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		timeout = time.Duration(seconds) * time.Second
	}

	if record, ok := b.consume(r.Context(), topicName, timeout); ok {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(record))
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		b.handleProduce(w, r)
	case http.MethodGet:
		b.handleConsume(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <port>\n", os.Args[0])
		os.Exit(1)
	}

	port := os.Args[1]
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	b := newBroker()
	if err := http.ListenAndServe(port, b); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
