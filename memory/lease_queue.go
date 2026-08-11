package memory

import (
	"cmp"
	"container/heap"
	"time"
)

type leasedEntry struct {
	identifier key
	expiresAt  time.Time
	index      int
}

type leaseQueue []*leasedEntry

func (queue leaseQueue) Len() int { return len(queue) }

func (queue leaseQueue) Less(left, right int) bool {
	return cmp.Or(
		queue[left].expiresAt.Compare(queue[right].expiresAt),
		cmp.Compare(queue[left].identifier.id, queue[right].identifier.id),
		cmp.Compare(queue[left].identifier.version, queue[right].identifier.version),
	) < 0
}

func (queue leaseQueue) Swap(left, right int) {
	queue[left], queue[right] = queue[right], queue[left]
	queue[left].index = left
	queue[right].index = right
}

func (queue *leaseQueue) Push(value any) {
	entry := value.(*leasedEntry)
	entry.index = len(*queue)
	*queue = append(*queue, entry)
}

func (queue *leaseQueue) Pop() any {
	old := *queue
	last := len(old) - 1
	entry := old[last]
	old[last] = nil
	*queue = old[:last]
	return entry
}

func (store *Store) setLease(identifier key, expiresAt time.Time) {
	if entry := store.leased[identifier]; entry != nil {
		entry.expiresAt = expiresAt
		heap.Fix(&store.leases, entry.index)
		return
	}
	entry := &leasedEntry{identifier: identifier, expiresAt: expiresAt}
	store.leased[identifier] = entry
	heap.Push(&store.leases, entry)
}

func (store *Store) clearLease(identifier key) {
	entry := store.leased[identifier]
	heap.Remove(&store.leases, entry.index)
	delete(store.leased, identifier)
}

func (store *Store) popExpiredLease(now time.Time) (key, bool) {
	if len(store.leases) == 0 || store.leases[0].expiresAt.After(now) {
		return key{}, false
	}
	entry := heap.Pop(&store.leases).(*leasedEntry)
	delete(store.leased, entry.identifier)
	return entry.identifier, true
}
