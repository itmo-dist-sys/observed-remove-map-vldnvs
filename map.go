package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode
	allNodeIDs []string
	state      MapState
	counter    uint64
	mu         sync.Mutex
	stopCh     chan struct{}
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	return &CRDTMapNode{
		BaseNode:   hive.NewBaseNode(id),
		allNodeIDs: allNodeIDs,
		state:      make(MapState),
	}
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	n.mu.Lock()
	n.stopCh = make(chan struct{})
	n.mu.Unlock()

	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}
	go n.syncLoop()
	return nil
}

// Stop gracefully shuts down the node and its sync loop.
func (n *CRDTMapNode) Stop() error {
	n.mu.Lock()
	if n.stopCh != nil {
		select {
		case <-n.stopCh:
		default:
			close(n.stopCh)
		}
	}
	n.mu.Unlock()
	return n.BaseNode.Stop()
}

func (n *CRDTMapNode) syncLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			state := n.copyState()
			n.mu.Unlock()
			for _, id := range n.allNodeIDs {
				if id == n.ID() {
					continue
				}
				msg := hive.NewMessage(n.ID(), id, state)
				_ = n.SendMessage(msg)
			}
		}
	}
}

func (n *CRDTMapNode) copyState() MapState {
	cp := make(MapState, len(n.state))
	for k, v := range n.state {
		cp[k] = v
	}
	return cp
}

func isNewer(a, b Version) bool {
	if a.Counter != b.Counter {
		return a.Counter > b.Counter
	}
	return a.NodeID > b.NodeID
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.counter++
	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   Version{Counter: n.counter, NodeID: n.ID()},
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	entry, exists := n.state[k]
	if !exists || entry.Tombstone {
		return "", false
	}
	return entry.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.counter++
	n.state[k] = StateEntry{
		Tombstone: true,
		Version:   Version{Counter: n.counter, NodeID: n.ID()},
	}
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, remoteEntry := range remote {
		localEntry, exists := n.state[k]
		if !exists || isNewer(remoteEntry.Version, localEntry.Version) {
			n.state[k] = remoteEntry
		}
		if remoteEntry.Version.Counter > n.counter {
			n.counter = remoteEntry.Version.Counter
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.copyState()
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	result := make(map[string]string)
	for k, entry := range n.state {
		if !entry.Tombstone {
			result[k] = entry.Value
		}
	}
	return result
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	remote, ok := msg.Payload.(MapState)
	if !ok {
		return nil
	}
	n.Merge(remote)
	return nil
}
