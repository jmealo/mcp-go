package server

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/mark3labs/mcp-go/mcp"
)

// Errors a TaskStore returns. Implementations must return these (or errors
// wrapping them) so the server can tell the cases apart.
var (
	// ErrTaskNotFound: no record with that ID.
	ErrTaskNotFound = errors.New("task not found")
	// ErrTaskExpired: the record exists but its ExpiresAt has passed.
	ErrTaskExpired = errors.New("task has expired")
	// ErrTaskExists: Create was called for an ID that is already stored.
	ErrTaskExists = errors.New("task already exists")
	// ErrTaskTerminal: Update was called for a record that is already in a
	// terminal status. A terminal record never changes again.
	ErrTaskTerminal = errors.New("task is in a terminal status")
	// ErrTaskConflict: Update's Revision does not match the stored record.
	ErrTaskConflict = errors.New("task revision conflict")
)

// TaskRecord is the durable state of one task: everything a server needs to
// serve tasks/get, tasks/result and tasks/list for it, and to run it again
// after a restart. Cancel functions and wait channels stay in the process
// that runs the task and are never stored.
type TaskRecord struct {
	Task      mcp.Task  `json:"task"`
	SessionID string    `json:"sessionId,omitempty"`
	ToolName  string    `json:"toolName"`
	CreatedAt time.Time `json:"createdAt"`
	// Modern is true for a task created by a 2026-07-28 (SEP-2663) request.
	Modern bool `json:"modern,omitempty"`
	// Protocol is the creating request's protocol info, restored on resume.
	Protocol *RequestProtocolInfo `json:"protocol,omitempty"`
	// Request is the original tools/call request, re-run on resume.
	Request json.RawMessage `json:"request,omitempty"`
	// Result is the tool result (a CallToolResult) once the task completed.
	Result json.RawMessage `json:"result,omitempty"`
	// Error is set once the task failed or was cancelled.
	Error *TaskRecordError `json:"error,omitempty"`
	// ExpiresAt is when the record stops being served; nil means never.
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	// ResumeState is opaque handler state set with SetTaskResumeState.
	ResumeState json.RawMessage `json:"resumeState,omitempty"`
	// InputRequests are the requests an input_required task waits on.
	InputRequests mcp.InputRequests `json:"inputRequests,omitempty"`
	// DeliveredInputKeys are the inputRequests keys surfaced so far; SEP-2663
	// requires them unique over the task's lifetime.
	DeliveredInputKeys []string `json:"deliveredInputKeys,omitempty"`
	// Owner identifies the server that last created or claimed the task.
	Owner string `json:"owner,omitempty"`
	// Revision is set by the store: 1 on Create, +1 on each Update. Update
	// succeeds only when the caller's Revision equals the stored one.
	Revision uint64 `json:"revision"`
}

// TaskRecordError is a failed or cancelled task's error, as the client sees it.
type TaskRecordError struct {
	Details   mcp.JSONRPCErrorDetails `json:"details"`
	Cancelled bool                    `json:"cancelled,omitempty"`
}

// TaskStore holds task records. Implementations must be safe for concurrent
// use, and Update must be atomic: compare Revision, reject terminal records,
// and write, as one step.
type TaskStore interface {
	// Create stores a new record with Revision 1. ErrTaskExists if the ID is taken.
	Create(ctx context.Context, rec TaskRecord) (TaskRecord, error)
	// Get returns the record. ErrTaskNotFound, or ErrTaskExpired once
	// ExpiresAt has passed.
	Get(ctx context.Context, taskID string) (TaskRecord, error)
	// Update replaces a non-terminal record whose Revision equals
	// rec.Revision and returns it with the Revision incremented. ErrTaskTerminal
	// if the stored record is terminal, ErrTaskConflict on a Revision mismatch,
	// ErrTaskNotFound or ErrTaskExpired otherwise.
	Update(ctx context.Context, rec TaskRecord) (TaskRecord, error)
	// Delete removes the record. Deleting a missing record is not an error.
	Delete(ctx context.Context, taskID string) error
	// ListBySession returns unexpired records visible to sessionID: those of
	// that session and those without a session. "" returns all records.
	ListBySession(ctx context.Context, sessionID string) ([]TaskRecord, error)
	// ListNonTerminal returns every unexpired record not in a terminal status.
	ListNonTerminal(ctx context.Context) ([]TaskRecord, error)
	// DeleteExpired deletes every record whose ExpiresAt is not after now and
	// returns how many it deleted.
	DeleteExpired(ctx context.Context, now time.Time) (int, error)
}

// WithTaskStore makes tasks durable in store: the server writes every task
// state change there before it reports it, serves tasks it does not run from
// there, and can resume unfinished tasks with ResumeTasks. Without it, tasks
// live only in this process.
//
// Task records keep the creating session's ID, and a task is only visible to
// that session (or to requests without one). After a restart, a stateful
// client that comes back with a new session ID cannot reach its tasks;
// keep session IDs stable across restarts (a persistent SessionIdManager)
// or use stateless requests.
//
// Call StopTaskStore when shutting the server down.
func WithTaskStore(store TaskStore) ServerOption {
	return func(s *MCPServer) {
		s.taskStore = store
		s.taskStoreCtx, s.taskStoreStop = context.WithCancel(context.Background())
		s.remoteTaskPoll = time.Second
		s.taskOwner = uuid.NewString()
		s.taskStoreRetryMin = 100 * time.Millisecond
		s.taskStoreRetryMax = 30 * time.Second
		s.taskPurgeInterval = time.Minute
	}
}

// MemoryTaskStore is an in-process TaskStore. It does not survive the
// process, but several servers in one process can share it, and it is the
// reference for the contract suite in server/taskstoretest.
type MemoryTaskStore struct {
	mu      sync.Mutex
	records map[string]TaskRecord
	now     func() time.Time
}

// NewMemoryTaskStore returns an empty store. now is the clock expiry is
// judged by; nil means time.Now.
func NewMemoryTaskStore(now func() time.Time) *MemoryTaskStore {
	if now == nil {
		now = time.Now
	}
	return &MemoryTaskStore{records: make(map[string]TaskRecord), now: now}
}

func cloneRecord(rec TaskRecord) TaskRecord {
	rec.Request = slices.Clone(rec.Request)
	rec.Result = slices.Clone(rec.Result)
	rec.ResumeState = slices.Clone(rec.ResumeState)
	rec.DeliveredInputKeys = slices.Clone(rec.DeliveredInputKeys)
	rec.InputRequests = maps.Clone(rec.InputRequests)
	if rec.Error != nil {
		e := *rec.Error
		rec.Error = &e
	}
	if rec.ExpiresAt != nil {
		t := *rec.ExpiresAt
		rec.ExpiresAt = &t
	}
	if rec.Protocol != nil {
		p := *rec.Protocol
		rec.Protocol = &p
	}
	return rec
}

func (m *MemoryTaskStore) expired(rec TaskRecord) bool {
	return rec.ExpiresAt != nil && !m.now().Before(*rec.ExpiresAt)
}

func (m *MemoryTaskStore) Create(_ context.Context, rec TaskRecord) (TaskRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.records[rec.Task.TaskId]; ok {
		return TaskRecord{}, ErrTaskExists
	}
	rec = cloneRecord(rec)
	rec.Revision = 1
	m.records[rec.Task.TaskId] = rec
	return cloneRecord(rec), nil
}

func (m *MemoryTaskStore) Get(_ context.Context, taskID string) (TaskRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[taskID]
	if !ok {
		return TaskRecord{}, ErrTaskNotFound
	}
	if m.expired(rec) {
		return TaskRecord{}, ErrTaskExpired
	}
	return cloneRecord(rec), nil
}

func (m *MemoryTaskStore) Update(_ context.Context, rec TaskRecord) (TaskRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stored, ok := m.records[rec.Task.TaskId]
	switch {
	case !ok:
		return TaskRecord{}, ErrTaskNotFound
	case m.expired(stored):
		return TaskRecord{}, ErrTaskExpired
	case stored.Task.Status.IsTerminal():
		return TaskRecord{}, ErrTaskTerminal
	case stored.Revision != rec.Revision:
		return TaskRecord{}, ErrTaskConflict
	}
	rec = cloneRecord(rec)
	rec.Revision = stored.Revision + 1
	m.records[rec.Task.TaskId] = rec
	return cloneRecord(rec), nil
}

func (m *MemoryTaskStore) Delete(_ context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, taskID)
	return nil
}

func (m *MemoryTaskStore) ListBySession(_ context.Context, sessionID string) ([]TaskRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []TaskRecord
	for _, rec := range m.records {
		if m.expired(rec) {
			continue
		}
		if sessionID == "" || rec.SessionID == "" || rec.SessionID == sessionID {
			out = append(out, cloneRecord(rec))
		}
	}
	return out, nil
}

func (m *MemoryTaskStore) DeleteExpired(_ context.Context, now time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for id, rec := range m.records {
		if rec.ExpiresAt != nil && !now.Before(*rec.ExpiresAt) {
			delete(m.records, id)
			n++
		}
	}
	return n, nil
}

func (m *MemoryTaskStore) ListNonTerminal(_ context.Context) ([]TaskRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []TaskRecord
	for _, rec := range m.records {
		if !m.expired(rec) && !rec.Task.Status.IsTerminal() {
			out = append(out, cloneRecord(rec))
		}
	}
	return out, nil
}
