package server

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mark3labs/mcp-go/mcp"
)

// After B resumes a task, A (still running it) loses ownership: A's next
// save fails, A's handler is cancelled, and B's state is what the store holds.
func TestTaskStore_ResumeClaimsOwnership(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	a := durableServer(store)
	saveAgain := make(chan struct{})
	aSave := make(chan error, 1)
	aCancelled := make(chan struct{})
	a.AddTool(taskTool("work"), func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		require.NoError(t, SetTaskResumeState(ctx, json.RawMessage(`"a-1"`)))
		<-saveAgain
		aSave <- SetTaskResumeState(ctx, json.RawMessage(`"a-2"`))
		<-ctx.Done()
		aSave <- SetTaskResumeState(ctx, json.RawMessage(`"a-3"`))
		close(aCancelled)
		return nil, ctx.Err()
	})
	id := startLegacyTask(t, a, "work")
	waitStored(t, store, id, func(r TaskRecord) bool { return string(r.ResumeState) == `"a-1"` })

	b := durableServer(store)
	bSaved := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	b.AddTool(taskTool("work"), func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		require.NoError(t, SetTaskResumeState(ctx, json.RawMessage(`"b-1"`)))
		close(bSaved)
		<-release
		return mcp.NewToolResultText("b"), nil
	})
	report, err := b.ResumeTasks(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{id}, report.Resumed)
	<-bSaved
	rec, _ := store.Get(context.Background(), id)
	assert.Equal(t, b.taskOwner, rec.Owner, "B claimed the record")

	close(saveAgain)
	assert.ErrorIs(t, <-aSave, ErrTaskOwnershipLost)
	assert.ErrorIs(t, <-aSave, ErrTaskOwnershipLost, "and keeps saying so after the drop")
	select {
	case <-aCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("A's handler was not cancelled after losing ownership")
	}
	rec, _ = store.Get(context.Background(), id)
	assert.Equal(t, `"b-1"`, string(rec.ResumeState), "B's state wins")
	a.tasksMu.RLock()
	_, aHolds := a.tasks[id]
	a.tasksMu.RUnlock()
	assert.False(t, aHolds, "A dropped its entry")
	task, _, err := a.getTask(t.Context(), id, TaskOpGet)
	require.NoError(t, err)
	assert.Equal(t, mcp.TaskStatusWorking, task.Status, "A now serves the task from the store")
}

// gatedStore fails terminal writes until open is closed, and reports each
// refused attempt on attempts.
type gatedStore struct {
	TaskStore
	open     chan struct{}
	attempts chan TaskRecord
}

func newGatedStore() *gatedStore {
	return &gatedStore{TaskStore: NewMemoryTaskStore(nil), open: make(chan struct{}), attempts: make(chan TaskRecord, 64)}
}

func (g *gatedStore) Update(ctx context.Context, rec TaskRecord) (TaskRecord, error) {
	if rec.Task.Status.IsTerminal() {
		select {
		case <-g.open:
		default:
			select {
			case g.attempts <- rec:
			default:
			}
			return TaskRecord{}, errors.New("disk full")
		}
	}
	return g.TaskStore.Update(ctx, rec)
}

// An outcome the store refuses is retried, and until it is stored the task
// reads as running: no terminal state is reported that the store does not hold.
func TestTaskStore_UnsavedOutcomeIsRetriedNotReported(t *testing.T) {
	store := newGatedStore()
	s := durableServer(store)
	t.Cleanup(s.StopTaskStore)
	s.taskStoreRetryMin, s.taskStoreRetryMax = time.Millisecond, time.Millisecond
	s.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("kept"), nil
	})
	id := startLegacyTask(t, s, "work")
	<-store.attempts
	<-store.attempts // refused twice: still unsaved while we look
	task, _, err := s.getTask(t.Context(), id, TaskOpGet)
	require.NoError(t, err)
	assert.Equal(t, mcp.TaskStatusWorking, task.Status, "not reported before it is stored")
	rec, _ := store.Get(context.Background(), id)
	assert.Equal(t, mcp.TaskStatusWorking, rec.Task.Status)

	close(store.open)
	done := waitLocalTerminal(t, s, id)
	assert.Equal(t, mcp.TaskStatusCompleted, done.Status, "the original result, once stored")
	rec, _ = store.Get(context.Background(), id)
	assert.Equal(t, mcp.TaskStatusCompleted, rec.Task.Status)
	res, reqErr := s.handleTaskResult(t.Context(), 1, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: id}})
	require.Nil(t, reqErr)
	assert.Equal(t, "kept", res.Content[0].(mcp.TextContent).Text)
}

// When expiry removes a task whose outcome is still unsaved, its
// concurrency slot and its waiters are released.
func TestTaskStore_ExpiryDuringRetryReleasesSlot(t *testing.T) {
	store := newGatedStore()
	s := NewMCPServer("s", "1", WithTaskCapabilities(true, true, true), WithTaskStore(store), WithMaxConcurrentTasks(1))
	t.Cleanup(s.StopTaskStore)
	s.taskStoreRetryMin, s.taskStoreRetryMax = time.Millisecond, time.Millisecond
	s.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("unsaved"), nil
	})
	ttl := int64(50)
	res, reqErr := s.handleToolCall(t.Context(), 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "work", Task: &mcp.TaskParams{TTL: &ttl}}})
	require.Nil(t, reqErr)
	id := res.(*mcp.CreateTaskResult).Task.TaskId
	<-store.attempts
	s.tasksMu.RLock()
	entry := s.tasks[id]
	s.tasksMu.RUnlock()
	require.NotNil(t, entry)
	select {
	case <-entry.done:
	case <-time.After(5 * time.Second):
		t.Fatal("done was not closed after expiry during the retry")
	}
	s.tasksMu.RLock()
	active := s.activeTasks
	s.tasksMu.RUnlock()
	assert.Zero(t, active, "the slot is released")
	// The slot is usable again.
	_, reqErr = s.handleToolCall(t.Context(), 2, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "work", Task: &mcp.TaskParams{TTL: &ttl}}})
	assert.Nil(t, reqErr)
}

// blockingTerminalStore holds the first terminal write inside Update until
// release is closed, then lets it succeed.
type blockingTerminalStore struct {
	TaskStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingTerminalStore) Update(ctx context.Context, rec TaskRecord) (TaskRecord, error) {
	if rec.Task.Status.IsTerminal() {
		first := false
		b.once.Do(func() { first = true })
		if first {
			close(b.entered)
			<-b.release
		}
	}
	return b.TaskStore.Update(ctx, rec)
}

// A cancel racing a terminal write that then succeeds is refused, and this
// server reports exactly what the store holds.
func TestTaskStore_CancelRacingTerminalWrite(t *testing.T) {
	store := &blockingTerminalStore{TaskStore: NewMemoryTaskStore(nil), entered: make(chan struct{}), release: make(chan struct{})}
	s := durableServer(store)
	t.Cleanup(s.StopTaskStore)
	s.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("done"), nil
	})
	id := startLegacyTask(t, s, "work")
	<-store.entered // the outcome is inside Update
	_, reqErr := s.handleCancelTask(t.Context(), 2, mcp.CancelTaskRequest{Params: mcp.CancelTaskParams{TaskId: id}})
	require.NotNil(t, reqErr, "a cancel is not acked while the outcome is being stored")
	assert.Equal(t, mcp.INTERNAL_ERROR, reqErr.code)
	assert.Contains(t, reqErr.err.Error(), "outcome pending")
	close(store.release)
	task := waitLocalTerminal(t, s, id)
	rec, _ := store.Get(context.Background(), id)
	assert.Equal(t, rec.Task.Status, task.Status, "tasks/get and the store agree")
	assert.Equal(t, mcp.TaskStatusCompleted, task.Status)
}

// A cancel while the outcome is still refused by the store gets an error,
// and the outcome then wins once stored.
func TestTaskStore_CancelDuringPendingOutcomeIsRefused(t *testing.T) {
	store := newGatedStore()
	s := durableServer(store)
	t.Cleanup(s.StopTaskStore)
	s.taskStoreRetryMin, s.taskStoreRetryMax = time.Millisecond, time.Millisecond
	s.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("the outcome"), nil
	})
	id := startLegacyTask(t, s, "work")
	<-store.attempts
	err := s.cancelTask(t.Context(), id, false)
	require.Error(t, err, "not acked: it could not be honoured")
	assert.ErrorIs(t, err, errTaskStoreFailure)
	close(store.open)
	task := waitLocalTerminal(t, s, id)
	assert.Equal(t, mcp.TaskStatusCompleted, task.Status)
	rec, _ := store.Get(context.Background(), id)
	assert.Equal(t, mcp.TaskStatusCompleted, rec.Task.Status, "store and memory agree")
}

// StopTaskStore during a pending outcome leaves no waiter blocked and no
// slot used, and reports nothing terminal (the store still has it running).
func TestTaskStore_StopDuringPendingOutcome(t *testing.T) {
	store := newGatedStore()
	s := NewMCPServer("s", "1", WithTaskCapabilities(true, true, true), WithTaskStore(store), WithMaxConcurrentTasks(1))
	s.taskStoreRetryMin, s.taskStoreRetryMax = time.Hour, time.Hour
	s.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("unsaved"), nil
	})
	id := startLegacyTask(t, s, "work")
	<-store.attempts
	waiter := make(chan *requestError, 1)
	go func() {
		_, reqErr := s.handleTaskResult(context.Background(), 1, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: id}})
		waiter <- reqErr
	}()
	s.StopTaskStore()
	select {
	case reqErr := <-waiter:
		require.NotNil(t, reqErr)
		assert.Equal(t, mcp.INTERNAL_ERROR, reqErr.code)
		assert.Contains(t, reqErr.err.Error(), "server is stopping")
	case <-time.After(5 * time.Second):
		t.Fatal("a tasks/result waiter is still blocked after StopTaskStore")
	}
	require.Eventually(t, func() bool {
		s.tasksMu.RLock()
		defer s.tasksMu.RUnlock()
		return s.activeTasks == 0
	}, 5*time.Second, time.Millisecond, "the slot is released")
	rec, _ := store.Get(context.Background(), id)
	assert.Equal(t, mcp.TaskStatusWorking, rec.Task.Status, "left running for the next server")
}

// StopTaskStore ends a retry that would otherwise wait for an hour.
func TestTaskStore_StopInterruptsRetry(t *testing.T) {
	store := newGatedStore()
	s := durableServer(store)
	s.taskStoreRetryMin, s.taskStoreRetryMax = time.Hour, time.Hour
	s.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("x"), nil
	})
	id := startLegacyTask(t, s, "work")
	s.tasksMu.RLock()
	entry := s.tasks[id]
	s.tasksMu.RUnlock()
	returned := make(chan error, 1)
	go func() {
		_, err := s.storeWriteRetrying(entry, func(r *TaskRecord) { r.Task.Status = mcp.TaskStatusCompleted })
		returned <- err
	}()
	<-store.attempts
	<-store.attempts
	s.StopTaskStore()
	select {
	case err := <-returned:
		assert.ErrorIs(t, err, errTaskStoreStopped)
	case <-time.After(5 * time.Second):
		t.Fatal("the retry did not stop")
	}
	rec, _ := store.Get(context.Background(), id)
	assert.Equal(t, mcp.TaskStatusWorking, rec.Task.Status, "left for the next server")
}

// A client cancel sent to the old owner after another server claimed the
// task cancels it there, through the store, instead of a false ack.
func TestTaskStore_CancelToOldOwnerAfterClaim(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	a := durableServer(store)
	block := make(chan struct{})
	defer close(block)
	a.AddTool(taskTool("work"), func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return nil, ctx.Err()
	})
	id := startLegacyTask(t, a, "work")
	b := durableServer(store)
	bSave := make(chan error, 1)
	bTick := make(chan struct{})
	b.AddTool(taskTool("work"), func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		<-bTick
		bSave <- SetTaskResumeState(ctx, json.RawMessage(`"b"`))
		<-ctx.Done()
		return nil, ctx.Err()
	})
	report, err := b.ResumeTasks(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{id}, report.Resumed)
	t.Cleanup(b.StopTaskStore)

	require.NoError(t, a.cancelTask(t.Context(), id, false))
	rec, _ := store.Get(context.Background(), id)
	assert.Equal(t, mcp.TaskStatusCancelled, rec.Task.Status, "the cancel reached the store")
	close(bTick)
	assert.ErrorIs(t, <-bSave, ErrTaskTerminal, "B stops at its next write")
	assert.Equal(t, mcp.TaskStatusCancelled, waitLocalTerminal(t, b, id).Status)
}

// The purge timer lives with the server: it outlives the ResumeTasks
// context and stops at StopTaskStore.
func TestTaskStore_PurgeLivesWithServer(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	s := durableServer(store)
	s.taskPurgeInterval = 5 * time.Millisecond
	resumeCtx, cancel := context.WithCancel(context.Background())
	_, err := s.ResumeTasks(resumeCtx)
	require.NoError(t, err)
	cancel() // a startup timeout must not stop the purge

	expired := func(id string) {
		past := time.Now().Add(-time.Second)
		task := mcp.NewTask(id)
		_, err := store.Create(context.Background(), TaskRecord{Task: task, ToolName: "x", ExpiresAt: &past})
		require.NoError(t, err)
	}
	gone := func(id string) bool {
		_, err := store.Get(context.Background(), id)
		return errors.Is(err, ErrTaskNotFound)
	}
	expired("first")
	require.Eventually(t, func() bool { return gone("first") }, 5*time.Second, time.Millisecond, "purged after the resume context ended")

	s.StopTaskStore()
	time.Sleep(20 * time.Millisecond) // let a tick in flight finish
	expired("second")
	time.Sleep(50 * time.Millisecond) // ten intervals
	assert.False(t, gone("second"), "no purge after StopTaskStore")
}

// A task that answered an input round and went back to working resumes from
// that round, not from the original request (which would re-issue its key).
func TestTaskStore_ResumeAfterAnsweredRound(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	ctx := modernTasksCtx(t.Context())
	a := NewMCPServer("a", "1", WithTaskCapabilities(true, true, true), WithTaskStore(store))
	block := make(chan struct{})
	defer close(block)
	a.AddTool(taskTool("rounds"), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if req.Params.RequestState == "after-q1" {
			_ = SetTaskResumeState(ctx, json.RawMessage(`"in round 2"`))
			<-block // A dies here
			return nil, ctx.Err()
		}
		return NewInputRequestBuilder("after-q1").Elicit("q1", mcp.ElicitationParams{
			Mode: mcp.ElicitationModeForm, Message: "q1", RequestedSchema: map[string]any{"type": "object"},
		}).ToolResult(), nil
	})
	created, reqErr := a.handleToolCall(ctx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "rounds"}})
	require.Nil(t, reqErr)
	id := created.(*mcp.CreateTaskResult).Task.TaskId
	waitStored(t, store, id, func(r TaskRecord) bool { return r.Task.Status == mcp.TaskStatusInputRequired })
	_, reqErr = a.handleUpdateTask(ctx, 2, mcp.UpdateTaskRequest{Params: mcp.UpdateTaskParams{TaskId: id, InputResponses: answerFor("q1")}})
	require.Nil(t, reqErr)
	waitStored(t, store, id, func(r TaskRecord) bool { return string(r.ResumeState) == `"in round 2"` })

	b := NewMCPServer("b", "1", WithTaskCapabilities(true, true, true), WithTaskStore(store))
	var gotState string
	var gotAnswer bool
	b.AddTool(taskTool("rounds"), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if req.Params.RequestState != "after-q1" {
			return NewInputRequestBuilder("after-q1").Elicit("q1", mcp.ElicitationParams{
				Mode: mcp.ElicitationModeForm, Message: "q1", RequestedSchema: map[string]any{"type": "object"},
			}).ToolResult(), nil
		}
		gotState = req.Params.RequestState
		_, gotAnswer = req.Params.InputResponses["q1"]
		return mcp.NewToolResultText("done after q1"), nil
	})
	report, err := b.ResumeTasks(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{id}, report.Resumed)
	rec := waitStored(t, store, id, func(r TaskRecord) bool { return r.Task.Status.IsTerminal() })
	assert.Equal(t, mcp.TaskStatusCompleted, rec.Task.Status, rec.Task.StatusMessage)
	assert.Equal(t, "after-q1", gotState)
	assert.True(t, gotAnswer, "the stored answer is passed again")
}

// A modern tasks/get on another server shows an input_required task's requests.
func TestTaskStore_RemoteInputRequiredShowsRequests(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	ctx := modernTasksCtx(t.Context())
	a := NewMCPServer("a", "1", WithTaskCapabilities(true, true, true), WithTaskStore(store))
	inputRoundTool(a, "ask", []string{"q1"})
	created, reqErr := a.handleToolCall(ctx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "ask"}})
	require.Nil(t, reqErr)
	id := created.(*mcp.CreateTaskResult).Task.TaskId
	waitStored(t, store, id, func(r TaskRecord) bool { return r.Task.Status == mcp.TaskStatusInputRequired })

	b := NewMCPServer("b", "1", WithTaskCapabilities(true, true, true), WithTaskStore(store))
	got, reqErr := b.handleGetTask(ctx, 2, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: id}})
	require.Nil(t, reqErr)
	assert.Equal(t, mcp.TaskStatusInputRequired, got.Status)
	assert.Contains(t, got.InputRequests, "q1")
	_, reqErr = b.handleUpdateTask(ctx, 3, mcp.UpdateTaskRequest{Params: mcp.UpdateTaskParams{TaskId: id, InputResponses: answerFor("q1")}})
	require.NotNil(t, reqErr, "answers cannot be delivered to another server's task")
	assert.Equal(t, mcp.INTERNAL_ERROR, reqErr.code)
}

// brokenGetStore fails Get with an error that is none of the sentinels.
type brokenGetStore struct {
	TaskStore
	fails atomic.Int32 // remaining failures; <0 means always
}

func (b *brokenGetStore) Get(ctx context.Context, id string) (TaskRecord, error) {
	if n := b.fails.Add(-1); n >= 0 || b.fails.Load() < -1000 {
		return TaskRecord{}, errors.New("connection reset")
	}
	return b.TaskStore.Get(ctx, id)
}

func TestTaskStore_StoreErrorsAreInternal(t *testing.T) {
	store := &brokenGetStore{TaskStore: NewMemoryTaskStore(nil)}
	store.fails.Store(-2000)
	s := durableServer(store)
	_, reqErr := s.handleGetTask(t.Context(), 1, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: "x"}})
	require.NotNil(t, reqErr)
	assert.Equal(t, mcp.INTERNAL_ERROR, reqErr.code)
	_, reqErr = s.handleTaskResult(t.Context(), 2, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: "x"}})
	require.NotNil(t, reqErr)
	assert.Equal(t, mcp.INTERNAL_ERROR, reqErr.code)
	_, reqErr = s.handleCancelTask(t.Context(), 3, mcp.CancelTaskRequest{Params: mcp.CancelTaskParams{TaskId: "x"}})
	require.NotNil(t, reqErr)
	assert.Equal(t, mcp.INTERNAL_ERROR, reqErr.code)
	// A missing task is still the caller's error.
	ok := durableServer(NewMemoryTaskStore(nil))
	_, reqErr = ok.handleGetTask(t.Context(), 4, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: "x"}})
	require.NotNil(t, reqErr)
	assert.Equal(t, mcp.INVALID_PARAMS, reqErr.code)
}

// scriptedGetStore fails the Gets it is told to, counted from arm.
type scriptedGetStore struct {
	TaskStore
	mu     sync.Mutex
	calls  int
	failOn map[int]bool
	failed chan struct{}
}

func (g *scriptedGetStore) Get(ctx context.Context, id string) (TaskRecord, error) {
	g.mu.Lock()
	g.calls++
	fail := g.failOn[g.calls]
	g.mu.Unlock()
	if fail {
		g.failed <- struct{}{}
		return TaskRecord{}, errors.New("connection reset")
	}
	return g.TaskStore.Get(ctx, id)
}

// tasks/result on another server's task rides out transient store errors,
// fewer than remoteTaskPollErrors in a row.
func TestTaskStore_ResultWaitToleratesTransientErrors(t *testing.T) {
	inner := NewMemoryTaskStore(nil)
	a := durableServer(inner)
	release := make(chan struct{})
	a.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		<-release
		return mcp.NewToolResultText("ok"), nil
	})
	id := startLegacyTask(t, a, "work")
	// Get 1 is the lookup, then polls; polls 2 and 3 fail (2 < remoteTaskPollErrors).
	store := &scriptedGetStore{TaskStore: inner, failOn: map[int]bool{2: true, 3: true}, failed: make(chan struct{}, 2)}
	b := durableServer(store)
	b.remoteTaskPoll = time.Millisecond
	go func() {
		<-store.failed
		<-store.failed
		close(release) // A finishes only after both failures were seen
	}()
	res, reqErr := b.handleTaskResult(t.Context(), 1, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: id}})
	require.Nil(t, reqErr)
	assert.Equal(t, "ok", res.Content[0].(mcp.TextContent).Text)
}

func TestTaskStore_ResumeRespectsMaxConcurrentTasks(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	one := record(t, store, "one", "work", mcp.TaskStatusWorking, `{"method":"tools/call","params":{"name":"work"}}`)
	two := record(t, store, "two", "work", mcp.TaskStatusWorking, `{"method":"tools/call","params":{"name":"work"}}`)
	b := NewMCPServer("b", "1", WithTaskCapabilities(true, true, true), WithTaskStore(store), WithMaxConcurrentTasks(1))
	release := make(chan struct{})
	defer close(release)
	b.AddTool(taskTool("work"), func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		<-release
		return mcp.NewToolResultText("x"), nil
	})
	report, err := b.ResumeTasks(t.Context())
	require.NoError(t, err)
	t.Cleanup(b.StopTaskStore)
	assert.Len(t, report.Resumed, 1)
	assert.Len(t, report.OverLimit, 1)
	assert.ElementsMatch(t, []string{one, two}, append(report.Resumed, report.OverLimit...))
	left, _ := store.Get(context.Background(), report.OverLimit[0])
	assert.Equal(t, mcp.TaskStatusFailed, left.Task.Status, "a task over the limit fails, rather than stay working with no runner")
	assert.Contains(t, left.Task.StatusMessage, "over the concurrent task limit at resume")
}

func TestTaskStore_ResumePurgesExpired(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	past := time.Now().Add(-time.Minute)
	task := mcp.NewTask("old")
	task.Status = mcp.TaskStatusCompleted
	_, err := store.Create(context.Background(), TaskRecord{Task: task, ToolName: "work", ExpiresAt: &past})
	require.NoError(t, err)
	b := durableServer(store)
	report, err := b.ResumeTasks(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, report.Purged)
	_, err = store.Get(context.Background(), "old")
	assert.ErrorIs(t, err, ErrTaskNotFound)
}

// failInputRequired fails writes that park a task in input_required.
type failInputRequired struct{ TaskStore }

func (f failInputRequired) Update(ctx context.Context, rec TaskRecord) (TaskRecord, error) {
	if rec.Task.Status == mcp.TaskStatusInputRequired {
		return TaskRecord{}, errors.New("disk full")
	}
	return f.TaskStore.Update(ctx, rec)
}

// A state change awaitTaskInput cannot store fails the task (not log-only).
func TestTaskStore_UnsavedInputStateFailsTheTask(t *testing.T) {
	store := failInputRequired{NewMemoryTaskStore(nil)}
	ctx := modernTasksCtx(t.Context())
	s := NewMCPServer("s", "1", WithTaskCapabilities(true, true, true), WithTaskStore(store))
	inputRoundTool(s, "ask", []string{"q1"})
	created, reqErr := s.handleToolCall(ctx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "ask"}})
	require.Nil(t, reqErr)
	id := created.(*mcp.CreateTaskResult).Task.TaskId
	rec := waitStored(t, store, id, func(r TaskRecord) bool { return r.Task.Status.IsTerminal() })
	assert.Equal(t, mcp.TaskStatusFailed, rec.Task.Status)
	assert.Contains(t, rec.Task.StatusMessage, "task state could not be stored")
}
