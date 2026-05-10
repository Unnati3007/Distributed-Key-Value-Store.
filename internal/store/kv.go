package store

import (
	"fmt"
	"sync"
)

// KV is a thread-safe in-memory key-value store.
type KV struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *KV {
	return &KV{data: make(map[string]string)}
}

func (kv *KV) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

func (kv *KV) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

func (kv *KV) Delete(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.data[key]
	delete(kv.data, key)
	return ok
}

func (kv *KV) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	snap := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		snap[k] = v
	}
	return snap
}

func (kv *KV) Restore(snap map[string]string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = snap
}

func (kv *KV) Len() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}

// Apply dispatches a Raft log command to the state machine.
func (kv *KV) Apply(op, key, value string) error {
	switch op {
	case "SET":
		kv.Set(key, value)
	case "DEL":
		kv.Delete(key)
	default:
		return fmt.Errorf("unknown op: %s", op)
	}
	return nil
}
