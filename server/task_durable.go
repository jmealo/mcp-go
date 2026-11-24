package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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

// ErrTaskOwnershipLost is returned by SetTaskResumeState when another
// server has resumed (claimed) the task; the handler's context is cancelled.
var ErrTaskOwnershipLost = errors.New("task ownership lost: another server resumed it")

// errTaskStoreFailure wraps store errors other than the sentinels, so request
// handlers answer INTERNAL_ERROR rather than INVALID_PARAMS.
var errTaskStoreFailure = errors.New("task store")

// errTaskStoreStopped: StopTaskStore ended a retry.
var errTaskStoreStopped = errors.New("task store stopped")

func storeFailure(err error) error { return fmt.Errorf("%w: %w", errTaskStoreFailure, err) }

// recordGone: the record was deleted or expired (normally by TTL cleanup).
func recordGone(err error) bool {
	return errors.Is(err, ErrTaskNotFound) || errors.Is(err, ErrTaskExpired)
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
// restart, and writes it to the TaskStore before returning. ErrTaskTerminal
// if the task has ended in the store (for example cancelled elsewhere), or
// an ownership error if another server resumed it; in both cases the
// handler's context is cancelled. Without a TaskStore the state is kept in
// memory only.
func SetTaskResumeState(ctx context.Context, state json.RawMessage) error {
	h, ok := ctx.Value(taskHandleKey{}).(*taskHandle)
	if !ok {
		return ErrNotInTask
	}
	s, entry := h.server, h.entry
	s.tasksMu.Lock()
	if entry.stopped {
		s.tasksMu.Unlock()
		return errServerStopping
	}
	if entry.dropped {
		s.tasksMu.Unlock()
		return ErrTaskOwnershipLost
	}
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
	owner := entry.owner
	if !entry.remote {
		owner = s.taskOwner
	}
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
		InputRequests:      maps.Clone(entry.inputRequests),
		DeliveredInputKeys: keys,
		Owner:              owner,
		OwnerID:            entry.ownerID,
		Revision:           entry.loadRevision(),
	})
}

// storeWrite writes entry's current state, changed by mutate, with
// compare-and-set on entry's revision. The record is built after storeMu is
// taken, so a write never carries an older snapshot than one before it.
// A conflict is re-read: a terminal record wins (returned with
// ErrTaskTerminal). For a task this process runs, a newer non-terminal
// record means another server took it over (ErrTaskOwnershipLost); for a
// read-only (remote) entry the write is retried once on top of it.
func (s *MCPServer) storeWrite(ctx context.Context, entry *taskEntry, mutate func(*TaskRecord)) (TaskRecord, error) {
	entry.storeMu.Lock()
	defer entry.storeMu.Unlock()
	ctx = context.WithoutCancel(ctx) // a state change must not be lost to its request ending
	s.tasksMu.RLock()
	rec := s.recordFromEntryLocked(entry)
	s.tasksMu.RUnlock()
	for attempt := 0; ; attempt++ {
		if mutate != nil {
			mutate(&rec)
		}
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
			return TaskRecord{}, gErr
		}
		entry.storeRevision(latest.Revision)
		if latest.Task.Status.IsTerminal() {
			return latest, ErrTaskTerminal
		}
		if !entry.remote || attempt > 0 {
			return latest, ErrTaskOwnershipLost
		}
		rec = latest
	}
}

// storeWriteRetrying is storeWrite for outcomes that must be stored before
// they are reported: other errors are retried with backoff (doubling up to
// taskStoreRetryMax, logged each time) until the write succeeds, the record
// is decided elsewhere or gone, or the task leaves this server (expiry).
// StopTaskStore ends the wait (errTaskStoreStopped).
func (s *MCPServer) storeWriteRetrying(entry *taskEntry, mutate func(*TaskRecord)) (TaskRecord, error) {
	delay := s.taskStoreRetryMin
	for {
		stored, err := s.storeWrite(context.Background(), entry, mutate)
		if err == nil || errors.Is(err, ErrTaskTerminal) || errors.Is(err, ErrTaskOwnershipLost) || recordGone(err) {
			return stored, err
		}
		if !s.holds(entry) {
			return stored, err
		}
		s.logTaskStore("task store: task outcome not saved; still reported as running, retrying",
			"task", entry.task.TaskId, "retry_in", delay, "error", err)
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-s.taskStoreCtx.Done():
			t.Stop()
			return stored, errTaskStoreStopped
		}
		delay = min(delay*2, s.taskStoreRetryMax)
	}
}

// errServerStopping answers waiters of tasks released by StopTaskStore.
// It wraps errTaskStoreFailure, so request handlers answer INTERNAL_ERROR.
var errServerStopping = fmt.Errorf("%w: server is stopping; the task's state is kept for the next server", errTaskStoreFailure)

// releaseStopped lets go of a task whose state StopTaskStore left unsaved:
// it frees the slot and wakes local waiters (they get errServerStopping).
// Nothing is reported as terminal: the store still has the task running.
func (s *MCPServer) releaseStopped(entry *taskEntry) {
	s.tasksMu.Lock()
	id := entry.task.TaskId
	if s.tasks[id] == entry {
		delete(s.tasks, id)
		if !entry.completed {
			s.activeTasks--
		}
	}
	if !entry.dropped && !entry.completed {
		entry.dropped, entry.stopped = true, true
		close(entry.done)
	}
	cancel := entry.cancelFunc
	s.tasksMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// commitStored commits locally the terminal record the store returned.
func (s *MCPServer) commitStored(ctx context.Context, entry *taskEntry, stored TaskRecord) {
	if stored.Task.Status == mcp.TaskStatusCancelled {
		s.commitCancel(ctx, entry, stored.Task.StatusMessage)
		return
	}
	result, err := hydrateOutcome(stored)
	s.commitComplete(entry, result, err)
}

// StopTaskStore is for process shutdown. It ends the server's task store
// work: the expired-record purge started by ResumeTasks, and the retries of
// unsaved task outcomes. Those tasks stay as the store has them (running),
// to be resumed by the next server; here they are released (their slots
// freed, and local tasks/result waiters answered "server is stopping").
// StreamableHTTPServer.Shutdown and SSEServer.Shutdown call it; with other
// transports, call it yourself. It is safe to call more than once. It is
// per MCPServer, not per transport: with one server behind several
// transports, the first Shutdown stops task store work for all of them.
func (s *MCPServer) StopTaskStore() {
	if s.taskStoreStop != nil {
		s.taskStoreStop()
	}
}

// holds reports whether entry is still this server's live entry for its task.
func (s *MCPServer) holds(entry *taskEntry) bool {
	s.tasksMu.RLock()
	defer s.tasksMu.RUnlock()
	return s.tasks[entry.task.TaskId] == entry && !entry.dropped
}

// dropLostTask stops a task another server has taken over: it cancels the
// handler and removes the entry, so readers here are served from the store.
func (s *MCPServer) dropLostTask(entry *taskEntry) {
	s.tasksMu.Lock()
	id := entry.task.TaskId
	if s.tasks[id] == entry {
		delete(s.tasks, id)
		if !entry.completed {
			s.activeTasks--
		}
	}
	first := !entry.dropped && !entry.completed
	if first {
		entry.dropped = true
		close(entry.done) // wakes local waiters, which re-read the task from the store
	}
	cancel := entry.cancelFunc
	s.tasksMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if first {
		s.logTaskStore("task store: another server resumed this task; stopping it here", "task", id)
	}
}

// persistLocalState writes entry's current non-terminal state. A record that
// ended elsewhere is adopted, and one another server took is dropped; both
// cancel the handler.
func (s *MCPServer) persistLocalState(ctx context.Context, entry *taskEntry) error {
	if s.taskStore == nil {
		return nil
	}
	s.tasksMu.RLock()
	ended := entry.completed || entry.dropped
	s.tasksMu.RUnlock()
	if ended {
		return nil
	}
	stored, err := s.storeWrite(ctx, entry, nil)
	switch {
	case errors.Is(err, ErrTaskTerminal):
		s.adoptTerminal(ctx, entry, stored)
	case errors.Is(err, ErrTaskOwnershipLost):
		s.dropLostTask(entry)
	case err != nil:
		return storeFailure(err)
	}
	return err
}

// failUnsavedState fails a task whose non-terminal state could not be stored.
func (s *MCPServer) failUnsavedState(entry *taskEntry, err error) {
	s.completeTask(entry, nil, fmt.Errorf("task state could not be stored: %w", err))
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
		inputRequests:   rec.InputRequests,
		owner:           rec.Owner,
		ownerID:         rec.OwnerID,
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
		return nil, storeFailure(err)
	}
	return hydrateEntry(rec), nil
}

// taskLookupError maps a task lookup failure to a request error: a store
// failure is the server's fault (INTERNAL_ERROR), anything else the caller's.
func taskLookupError(id any, err error) *requestError {
	code := mcp.INVALID_PARAMS
	if errors.Is(err, errTaskStoreFailure) {
		code = mcp.INTERNAL_ERROR
	}
	return &requestError{id: id, code: code, err: err}
}

const (
	// remoteTaskPollErrors is how many store errors in a row the wait tolerates.
	remoteTaskPollErrors = 3
)

// waitStoredTerminal blocks until the stored task is terminal or ctx ends.
// Up to remoteTaskPollErrors transient store errors in a row are tolerated.
func (s *MCPServer) waitStoredTerminal(ctx context.Context, taskID string) error {
	t := time.NewTicker(s.remoteTaskPoll)
	defer t.Stop()
	failures := 0
	for {
		rec, err := s.taskStore.Get(ctx, taskID)
		switch {
		case recordGone(err):
			return err
		case err != nil:
			if failures++; failures >= remoteTaskPollErrors || ctx.Err() != nil {
				return storeFailure(err)
			}
		case rec.Task.Status.IsTerminal():
			return nil
		default:
			failures = 0
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.taskStoreCtx.Done():
			return errServerStopping
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
	Resumed   []string // claimed and re-running on this server
	Failed    []string // ended as failed (tool gone, awaiting input, bad request)
	Held      []string // already running on this server
	Racing    []string // changed or claimed by another writer during the resume
	Expired   []string // past ExpiresAt; deleted
	OverLimit []string // failed: over WithMaxConcurrentTasks at resume
	Purged    int      // expired records deleted by DeleteExpired
}

// ResumeTasks re-runs every unfinished task in the TaskStore on this server.
// Call it once, after every tool is registered: a task whose tool is not
// registered yet fails. Each task is claimed first (a revision bump with
// this server as Owner), so a process still running it loses ownership at
// its next store write and stops. The resumed handler gets the latest
// request (the original, or the last answered input round) and its resume
// state (TaskResumeStateFromContext); expiry is re-armed from ExpiresAt and
// no created hook fires. A task awaiting client input fails: its pending
// responses were in the old process. ResumeTasks also deletes expired
// records, and keeps doing so every minute until StopTaskStore; tasks over
// WithMaxConcurrentTasks fail (reported as OverLimit).
func (s *MCPServer) ResumeTasks(ctx context.Context, opts ...ResumeOption) (ResumeReport, error) {
	var report ResumeReport
	if s.taskStore == nil {
		return report, nil
	}
	var cfg resumeConfig
	for _, o := range opts {
		o(&cfg)
	}
	var errs []error
	purged, err := s.taskStore.DeleteExpired(ctx, time.Now())
	if err != nil {
		errs = append(errs, fmt.Errorf("task store: delete expired: %w", err))
	}
	report.Purged = purged
	s.purgeOnce.Do(func() { go s.purgeExpiredTasks() })

	recs, err := s.taskStore.ListNonTerminal(ctx)
	if err != nil {
		return report, errors.Join(append(errs, fmt.Errorf("task store: list unfinished tasks: %w", err))...)
	}
	for _, rec := range recs {
		outcome, rErr := s.resumeTask(ctx, rec, cfg)
		id := rec.Task.TaskId
		if rErr != nil {
			errs = append(errs, fmt.Errorf("task %s: %w", id, rErr))
		}
		switch outcome {
		case resumeStarted:
			report.Resumed = append(report.Resumed, id)
		case resumeFailed:
			report.Failed = append(report.Failed, id)
		case resumeHeld:
			report.Held = append(report.Held, id)
		case resumeRacing:
			report.Racing = append(report.Racing, id)
		case resumeExpired:
			report.Expired = append(report.Expired, id)
		case resumeOverLimit:
			report.OverLimit = append(report.OverLimit, id)
		}
	}
	return report, errors.Join(errs...)
}

// purgeExpiredTasks deletes expired records every taskPurgeInterval until
// StopTaskStore.
func (s *MCPServer) purgeExpiredTasks() {
	ctx := s.taskStoreCtx
	t := time.NewTicker(s.taskPurgeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := s.taskStore.DeleteExpired(ctx, time.Now()); err != nil && ctx.Err() == nil {
				s.logTaskStore("task store: expired tasks not deleted", "error", err)
			}
		}
	}
}

type resumeOutcome int

const (
	resumeNone resumeOutcome = iota
	resumeStarted
	resumeFailed
	resumeHeld
	resumeRacing
	resumeExpired
	resumeOverLimit
)

// failStoredTask ends a record this process does not run as failed.
func (s *MCPServer) failStoredTask(ctx context.Context, rec TaskRecord, cause error) (resumeOutcome, error) {
	rec.Task.Status = mcp.TaskStatusFailed
	rec.Task.StatusMessage = cause.Error()
	rec.Task.LastUpdatedAt = time.Now().UTC().Format(time.RFC3339)
	rec.Error = recordError(cause)
	rec.Owner = s.taskOwner
	_, err := s.taskStore.Update(ctx, rec)
	switch {
	case err == nil:
		return resumeFailed, nil
	case errors.Is(err, ErrTaskTerminal) || errors.Is(err, ErrTaskConflict):
		return resumeRacing, nil
	case recordGone(err):
		return resumeExpired, nil
	}
	return resumeNone, err
}

func (s *MCPServer) resumeTask(ctx context.Context, rec TaskRecord, cfg resumeConfig) (resumeOutcome, error) {
	id := rec.Task.TaskId
	s.tasksMu.RLock()
	_, held := s.tasks[id]
	atLimit := s.maxConcurrentTasks != nil && *s.maxConcurrentTasks > 0 && s.activeTasks >= *s.maxConcurrentTasks
	s.tasksMu.RUnlock()
	if held {
		return resumeHeld, nil
	}
	if rec.ExpiresAt != nil && !time.Now().Before(*rec.ExpiresAt) {
		return resumeExpired, s.taskStore.Delete(ctx, id)
	}
	if rec.Task.Status == mcp.TaskStatusInputRequired {
		return s.failStoredTask(ctx, rec, fmt.Errorf("task was awaiting client input when its server stopped; start it again"))
	}
	var request mcp.CallToolRequest
	if err := json.Unmarshal(rec.Request, &request); err != nil || request.Params.Name == "" {
		return s.failStoredTask(ctx, rec, fmt.Errorf("task's stored request cannot be resumed"))
	}
	s.toolsMu.RLock()
	taskTool, isTaskTool := s.taskTools[rec.ToolName]
	regularTool, isRegularTool := s.tools[rec.ToolName]
	s.toolsMu.RUnlock()
	if !isTaskTool && (!isRegularTool || regularTool.Tool.Execution == nil ||
		(regularTool.Tool.Execution.TaskSupport != mcp.TaskSupportOptional &&
			regularTool.Tool.Execution.TaskSupport != mcp.TaskSupportRequired)) {
		return s.failStoredTask(ctx, rec, fmt.Errorf("tool %q is not registered for task execution on this server", rec.ToolName))
	}
	if atLimit {
		outcome, err := s.failStoredTask(ctx, rec, fmt.Errorf("over the concurrent task limit at resume; start it again"))
		if outcome == resumeFailed {
			outcome = resumeOverLimit
		}
		return outcome, err
	}

	runCtx := ctx
	if rec.Protocol != nil {
		p := *rec.Protocol
		runCtx = WithRequestProtocolInfo(runCtx, &p)
	}
	if cfg.prepare != nil {
		prepared, err := cfg.prepare(runCtx, rec)
		if err != nil {
			return s.failStoredTask(ctx, rec, err)
		}
		runCtx = prepared
	}

	// Claim the record before running it: the revision bump makes the next
	// write of any process still running this task fail as ownership lost.
	rec.Owner = s.taskOwner
	claimed, err := s.taskStore.Update(ctx, rec)
	switch {
	case errors.Is(err, ErrTaskConflict) || errors.Is(err, ErrTaskTerminal):
		return resumeRacing, nil
	case recordGone(err):
		return resumeExpired, nil
	case err != nil:
		return resumeNone, err
	}

	entry := &taskEntry{
		task:            claimed.Task,
		sessionID:       claimed.SessionID,
		toolName:        claimed.ToolName,
		createdAt:       claimed.CreatedAt,
		modernTask:      claimed.Modern,
		protocol:        claimed.Protocol,
		request:         claimed.Request,
		expiresAt:       claimed.ExpiresAt,
		resumeState:     claimed.ResumeState,
		done:            make(chan struct{}),
		inputReady:      make(chan struct{}, 1),
		createdHookDone: true,
	}
	entry.storeRevision(claimed.Revision)
	if len(claimed.DeliveredInputKeys) > 0 {
		entry.deliveredInputKeys = make(map[string]struct{}, len(claimed.DeliveredInputKeys))
		for _, k := range claimed.DeliveredInputKeys {
			entry.deliveredInputKeys[k] = struct{}{}
		}
	}

	// The claim above is the only way in: a second claimant gets a conflict.
	s.tasksMu.Lock()
	s.activeTasks++
	s.tasks[id] = entry
	s.tasksMu.Unlock()

	if claimed.ExpiresAt != nil {
		go s.scheduleTaskCleanup(id, entry, max(time.Until(*claimed.ExpiresAt).Milliseconds(), 1))
	}
	if isTaskTool {
		go s.executeTaskTool(runCtx, entry, taskTool, request)
	} else {
		go s.executeRegularToolAsTask(runCtx, entry, regularTool, request)
	}
	return resumeStarted, nil
}
