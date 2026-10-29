package saga

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type EventKind string

const (
	StepDone           EventKind = "step_done"
	StepFailed         EventKind = "step_failed"
	CompensationDone   EventKind = "compensation_done"
	CompensationFailed EventKind = "compensation_failed"
	Finished           EventKind = "finished"
)

// Event is one line of a saga's history. Data is the saga's data after the event, so a resumed
// run starts from the last recorded state.
type Event struct {
	Kind   EventKind       `json:"kind"`
	Step   int             `json:"step"`
	Data   json.RawMessage `json:"data,omitempty"`
	Error  string          `json:"error,omitempty"`
	Status Status          `json:"status,omitempty"`
	At     time.Time       `json:"at"`
}

// Log stores saga histories. Append must be durable before it returns: a saga resumes from
// exactly what Events returns.
type Log interface {
	Append(ctx context.Context, sagaID string, e Event) error
	Events(ctx context.Context, sagaID string) ([]Event, error)
}

// MemoryLog keeps histories in memory: resumable within one process, gone on restart.
type MemoryLog struct {
	mu     sync.Mutex
	events map[string][]Event
}

func NewMemoryLog() *MemoryLog { return &MemoryLog{events: map[string][]Event{}} }

func (l *MemoryLog) Append(_ context.Context, id string, e Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events[id] = append(l.events[id], e)
	return nil
}

func (l *MemoryLog) Events(_ context.Context, id string) ([]Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Event(nil), l.events[id]...), nil
}
