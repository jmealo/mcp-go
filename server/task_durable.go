package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

// storedTaskError is a task error read back from a TaskStore. It keeps the
// client-facing details and, for a cancelled task, unwraps to context.Canceled.
type storedTaskError struct {
	details   mcp.JSONRPCErrorDetails
	cancelled bool
}

func (e *storedTaskError) Error() string { return e.details.Message }

func (e *storedTaskError) Unwrap() error {
	if e.cancelled {
		return context.Canceled
	}
	return nil
}

func recordError(err error) *TaskRecordError {
	if err == nil {
		return nil
	}
	var stored *storedTaskError
	if errors.As(err, &stored) {
		return &TaskRecordError{Details: stored.details, Cancelled: stored.cancelled}
	}
	return &TaskRecordError{Details: *taskErrorDetails(err), Cancelled: errors.Is(err, context.Canceled)}
}

// taskHandle ties a running handler's context to its task.
type taskHandle struct {
	server *MCPServer
	entry  *taskEntry
}

type taskHandleKey struct{}

func withTaskHandle(ctx context.Context, s *MCPServer, entry *taskEntry) context.Context {
	return context.WithValue(ctx, taskHandleKey{}, &taskHandle{server: s, entry: entry})
}

// ErrNotInTask is returned by SetTaskResumeState outside a task's handler.
var ErrNotInTask = errors.New("not running as a task")

// TaskIDFromContext returns the ID of the task whose handler ctx belongs to.
func TaskIDFromContext(ctx context.Context) (string, bool) {
	h, ok := ctx.Value(taskHandleKey{}).(*taskHandle)
	if !ok {
		return "", false
	}
	return h.entry.task.TaskId, true
}

// TaskResumeStateFromContext returns the task's latest resume state: the
// value last passed to SetTaskResumeState, including before a restart when
// the task was resumed by ResumeTasks. nil if none was set.
func TaskResumeStateFromContext(ctx context.Context) json.RawMessage {
	h, ok := ctx.Value(taskHandleKey{}).(*taskHandle)
	if !ok {
		return nil
	}
	h.server.tasksMu.RLock()
	defer h.server.tasksMu.RUnlock()
	return slices.Clone(h.entry.resumeState)
}

// SetTaskResumeState records opaque state for resuming the task after a
// restart, and writes it to the TaskStore before returning. If the task has
// meanwhile ended in the store (for example cancelled by another server),
// the handler's context is cancelled and ErrTaskTerminal returned. Without a
// TaskStore the state is kept in memory only.
func SetTaskResumeState(ctx context.Context, state json.RawMessage) error {
	h, ok := ctx.Value(taskHandleKey{}).(*taskHandle)
	if !ok {
		return ErrNotInTask
	}
	s, entry := h.server, h.entry
	s.tasksMu.Lock()
	if entry.completed {
		s.tasksMu.Unlock()
		return ErrTaskTerminal
	}
	entry.resumeState = slices.Clone(state)
	s.tasksMu.Unlock()
	return s.persistLocalState(ctx, entry)
}

func (s *MCPServer) logTaskStore(msg string, args ...any) {
	l := s.requestLogger
	if l == nil {
		l = slog.Default()
	}
	l.Error(msg, args...)
}

// recordFromEntryLocked builds the entry's record. Caller holds tasksMu.
func (s *MCPServer) recordFromEntryLocked(entry *taskEntry) TaskRecord {
	keys := make([]string, 0, len(entry.deliveredInputKeys))
	for k := range entry.deliveredInputKeys {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return cloneRecord(TaskRecord{
		Task:               entry.task,
		SessionID:          entry.sessionID,
		ToolName:           entry.toolName,
		CreatedAt:          entry.createdAt,
		Modern:             entry.modernTask,
		Protocol:           entry.protocol,
		Request:            entry.request,
		ExpiresAt:          entry.expiresAt,
		ResumeState:        entry.resumeState,
		DeliveredInputKeys: keys,
		Revision:           entry.loadRevision(),
	})
}

// storeUpdate writes rec for entry with compare-and-set on entry's revision.
// Writes for one entry are serialized by entry.storeMu, so the last write is
// the latest snapshot. On a conflict it re-reads once: a terminal record wins
// (returned with ErrTaskTerminal); otherwise the write is retried on top.
func (s *MCPServer) storeUpdate(ctx context.Context, entry *taskEntry, rec TaskRecord) (TaskRecord, error) {
	entry.storeMu.Lock()
	defer entry.storeMu.Unlock()
	ctx = context.WithoutCancel(ctx) // a state change must not be lost to its request ending
	for attempt := 0; ; attempt++ {
		rec.Revision = entry.loadRevision()
		stored, err := s.taskStore.Update(ctx, rec)
		if err == nil {
			entry.storeRevision(stored.Revision)
			return stored, nil
		}
		if !errors.Is(err, ErrTaskConflict) && !errors.Is(err, ErrTaskTerminal) {
			return TaskRecord{}, err
		}
		latest, gErr := s.taskStore.Get(ctx, rec.Task.TaskId)
		if gErr != nil {
			return TaskRecord{}, errors.Join(err, gErr)
		}
		if latest.Task.Status.IsTerminal() {
			entry.storeRevision(latest.Revision)
			return latest, ErrTaskTerminal
		}
		if attempt > 0 {
			return TaskRecord{}, err
		}
		entry.storeRevision(latest.Revision)
	}
}

// persistLocalState writes entry's current non-terminal state. A record that
// ended elsewhere is adopted here and its handler cancelled.
func (s *MCPServer) persistLocalState(ctx context.Context, entry *taskEntry) error {
	if s.taskStore == nil {
		return nil
	}
	s.tasksMu.RLock()
	if entry.completed {
		s.tasksMu.RUnlock()
		return nil
	}
	rec := s.recordFromEntryLocked(entry)
	s.tasksMu.RUnlock()
	stored, err := s.storeUpdate(ctx, entry, rec)
	if errors.Is(err, ErrTaskTerminal) {
		s.adoptTerminal(ctx, entry, stored)
		return err
	}
	if err != nil {
		s.logTaskStore("task store: state not saved", "task", rec.Task.TaskId, "error", err)
	}
	return err
}

// adoptTerminal commits locally a terminal state that another writer stored first.
func (s *MCPServer) adoptTerminal(ctx context.Context, entry *taskEntry, stored TaskRecord) {
	if stored.Task.Status == mcp.TaskStatusCancelled {
		s.commitCancel(ctx, entry, stored.Task.StatusMessage)
		return
	}
	result, err := hydrateOutcome(stored)
	s.commitComplete(entry, result, err)
	// The handler may still run; the task has ended, so stop it.
	s.tasksMu.RLock()
	cancel := entry.cancelFunc
	s.tasksMu.RUnlock()
	if cancel != nil {
		cancel()
	}
}

// hydrateOutcome turns a terminal record's stored result or error back into
// what the server keeps in memory.
func hydrateOutcome(rec TaskRecord) (any, error) {
	if rec.Error != nil {
		return nil, &storedTaskError{details: rec.Error.Details, cancelled: rec.Error.Cancelled}
	}
	if len(rec.Result) == 0 {
		return nil, nil
	}
	raw := json.RawMessage(rec.Result)
	res, err := mcp.ParseCallToolResult(&raw)
	if err != nil {
		return nil, fmt.Errorf("stored task result is unreadable: %w", err)
	}
	return res, nil
}

// hydrateEntry builds a read-only entry for a task this process does not run.
func hydrateEntry(rec TaskRecord) *taskEntry {
	e := &taskEntry{
		task:            rec.Task,
		sessionID:       rec.SessionID,
		toolName:        rec.ToolName,
		createdAt:       rec.CreatedAt,
		modernTask:      rec.Modern,
		protocol:        rec.Protocol,
		request:         rec.Request,
		expiresAt:       rec.ExpiresAt,
		resumeState:     rec.ResumeState,
		remote:          true,
		createdHookDone: true,
		done:            make(chan struct{}),
	}
	e.storeRevision(rec.Revision)
	if rec.Task.Status.IsTerminal() {
		e.completed = true
		close(e.done)
		e.result, e.resultErr = hydrateOutcome(rec)
	}
	return e
}

// loadStoredEntry reads a task this process does not hold from the store.
func (s *MCPServer) loadStoredEntry(ctx context.Context, taskID string) (*taskEntry, error) {
	rec, err := s.taskStore.Get(ctx, taskID)
	switch {
	case errors.Is(err, ErrTaskExpired):
		return nil, fmt.Errorf("task has expired")
	case errors.Is(err, ErrTaskNotFound):
		return nil, fmt.Errorf("task not found")
	case err != nil:
		return nil, fmt.Errorf("task store: %w", err)
	}
	return hydrateEntry(rec), nil
}

// remoteTaskPoll is how often tasks/result re-reads a task another process runs.
const remoteTaskPoll = time.Second

// waitStoredTerminal blocks until the stored task is terminal or ctx ends.
func (s *MCPServer) waitStoredTerminal(ctx context.Context, taskID string) error {
	t := time.NewTicker(remoteTaskPoll)
	defer t.Stop()
	for {
		rec, err := s.taskStore.Get(ctx, taskID)
		if err != nil {
			return err
		}
		if rec.Task.Status.IsTerminal() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// ResumeOption configures ResumeTasks.
type ResumeOption func(*resumeConfig)

type resumeConfig struct {
	prepare func(context.Context, TaskRecord) (context.Context, error)
}

// WithResumeContext lets the application rebuild per-task context (for
// example the caller's identity, kept in ResumeState) before a resumed
// handler runs. An error fails the task with that error.
func WithResumeContext(prepare func(context.Context, TaskRecord) (context.Context, error)) ResumeOption {
	return func(c *resumeConfig) { c.prepare = prepare }
}

// ResumeReport says what ResumeTasks did with each unfinished record.
type ResumeReport struct {
	Resumed []string // re-running on this server
	Failed  []string // ended as failed (tool gone, awaiting input, bad request)
	Skipped []string // already held by this server
}

// ResumeTasks re-runs every unfinished task in the TaskStore on this server.
// Call it once after registering tools. Each resumed handler gets the
// original request and its last resume state (TaskResumeStateFromContext);
// expiry is re-armed from the stored ExpiresAt, and no created hook fires.
// A task whose tool is no longer registered, or that was awaiting client
// input (its responses were in the old process), fails with a clear error.
// It assumes no other live server runs these tasks.
func (s *MCPServer) ResumeTasks(ctx context.Context, opts ...ResumeOption) (ResumeReport, error) {
	var report ResumeReport
	if s.taskStore == nil {
		return report, nil
	}
	var cfg resumeConfig
	for _, o := range opts {
		o(&cfg)
	}
	recs, err := s.taskStore.ListNonTerminal(ctx)
	if err != nil {
		return report, fmt.Errorf("task store: list unfinished tasks: %w", err)
	}
	var errs []error
	for _, rec := range recs {
		id := rec.Task.TaskId
		s.tasksMu.RLock()
		_, held := s.tasks[id]
		s.tasksMu.RUnlock()
		if held {
			report.Skipped = append(report.Skipped, id)
			continue
		}
		resumed, rErr := s.resumeTask(ctx, rec, cfg)
		if rErr != nil {
			errs = append(errs, fmt.Errorf("task %s: %w", id, rErr))
		}
		if resumed {
			report.Resumed = append(report.Resumed, id)
		} else if rErr == nil {
			report.Failed = append(report.Failed, id)
		}
	}
	return report, errors.Join(errs...)
}

// failStoredTask ends a record this process does not run as failed.
func (s *MCPServer) failStoredTask(ctx context.Context, rec TaskRecord, cause error) error {
	now := time.Now()
	rec.Task.Status = mcp.TaskStatusFailed
	rec.Task.StatusMessage = cause.Error()
	rec.Task.LastUpdatedAt = now.UTC().Format(time.RFC3339)
	rec.Error = recordError(cause)
	_, err := s.taskStore.Update(ctx, rec)
	if errors.Is(err, ErrTaskTerminal) {
		return nil
	}
	return err
}

func (s *MCPServer) resumeTask(ctx context.Context, rec TaskRecord, cfg resumeConfig) (bool, error) {
	fail := func(cause error) (bool, error) { return false, s.failStoredTask(ctx, rec, cause) }
	if rec.Task.Status == mcp.TaskStatusInputRequired {
		return fail(fmt.Errorf("task was awaiting client input when its server stopped; start it again"))
	}
	var request mcp.CallToolRequest
	if err := json.Unmarshal(rec.Request, &request); err != nil || request.Params.Name == "" {
		return fail(fmt.Errorf("task's stored request cannot be resumed"))
	}
	s.toolsMu.RLock()
	taskTool, isTaskTool := s.taskTools[rec.ToolName]
	regularTool, isRegularTool := s.tools[rec.ToolName]
	s.toolsMu.RUnlock()
	if !isTaskTool && (!isRegularTool || regularTool.Tool.Execution == nil ||
		(regularTool.Tool.Execution.TaskSupport != mcp.TaskSupportOptional &&
			regularTool.Tool.Execution.TaskSupport != mcp.TaskSupportRequired)) {
		return fail(fmt.Errorf("tool %q is no longer registered for task execution", rec.ToolName))
	}

	runCtx := ctx
	if rec.Protocol != nil {
		p := *rec.Protocol
		runCtx = WithRequestProtocolInfo(runCtx, &p)
	}
	if cfg.prepare != nil {
		prepared, err := cfg.prepare(runCtx, rec)
		if err != nil {
			return fail(err)
		}
		runCtx = prepared
	}

	entry := &taskEntry{
		task:            rec.Task,
		sessionID:       rec.SessionID,
		toolName:        rec.ToolName,
		createdAt:       rec.CreatedAt,
		modernTask:      rec.Modern,
		protocol:        rec.Protocol,
		request:         rec.Request,
		expiresAt:       rec.ExpiresAt,
		resumeState:     rec.ResumeState,
		done:            make(chan struct{}),
		inputReady:      make(chan struct{}, 1),
		createdHookDone: true,
	}
	entry.storeRevision(rec.Revision)
	if len(rec.DeliveredInputKeys) > 0 {
		entry.deliveredInputKeys = make(map[string]struct{}, len(rec.DeliveredInputKeys))
		for _, k := range rec.DeliveredInputKeys {
			entry.deliveredInputKeys[k] = struct{}{}
		}
	}
	var remaining time.Duration
	if rec.ExpiresAt != nil {
		remaining = time.Until(*rec.ExpiresAt)
		if remaining <= 0 {
			return false, s.taskStore.Delete(ctx, rec.Task.TaskId)
		}
	}

	s.tasksMu.Lock()
	if _, held := s.tasks[rec.Task.TaskId]; held {
		s.tasksMu.Unlock()
		return false, nil
	}
	s.activeTasks++
	s.tasks[rec.Task.TaskId] = entry
	s.tasksMu.Unlock()

	if rec.ExpiresAt != nil {
		go s.scheduleTaskCleanup(rec.Task.TaskId, entry, remaining.Milliseconds())
	}
	if isTaskTool {
		go s.executeTaskTool(runCtx, entry, taskTool, request)
	} else {
		go s.executeRegularToolAsTask(runCtx, entry, regularTool, request)
	}
	return true, nil
}
