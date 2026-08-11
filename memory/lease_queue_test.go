package memory

import (
	"container/heap"
	"testing"
	"time"
)

func TestLeaseQueueUsesStrictDeterministicExpiryAndIdentityOrder(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 15, 0, 0, 0, time.UTC)
	queue := leaseQueue{
		{identifier: key{id: "same", version: 2}, expiresAt: now},
		{identifier: key{id: "later", version: 1}, expiresAt: now.Add(time.Second)},
		{identifier: key{id: "same", version: 1}, expiresAt: now},
		{identifier: key{id: "earlier", version: 1}, expiresAt: now},
	}
	if queue.Less(0, 0) {
		t.Fatal("lease ordering was not strict for one identity")
	}
	heap.Init(&queue)
	want := []key{
		{id: "earlier", version: 1},
		{id: "same", version: 1},
		{id: "same", version: 2},
		{id: "later", version: 1},
	}
	for index, identifier := range want {
		entry := heap.Pop(&queue).(*leasedEntry)
		if entry.identifier != identifier {
			t.Fatalf("pop %d identity = %+v, want %+v", index, entry.identifier, identifier)
		}
	}
	if queue.Len() != 0 {
		t.Fatalf("queue length = %d, want 0", queue.Len())
	}
}
