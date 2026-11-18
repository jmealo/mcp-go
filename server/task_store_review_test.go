package server

import (
	"context"
	"encoding/json"
	"errors"
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
	assert.ErrorIs(t, <-aSave, errTaskOwnershipLost)
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
	task, _, err := a.getTask(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, mcp.TaskStatusWorking, task.Status, "A now serves the task from the store")
}

// flakyTerminalStore fails the first n writes of a terminal status.
type flakyTerminalStore struct {
	TaskStore
	failures atomic.Int32
	attempts atomic.Int32
}

func (f *flakyTerminalStore) Update(ctx context.Context, rec TaskRecord) (TaskRecord, error) {
	if rec.Task.Status.IsTerminal() {
		f.attempts.Add(1)
		if f.failures.Add(-1) >= 0 {
			return TaskRecord{}, errors.New("disk full")
		}
	}
	return f.TaskStore.Update(ctx, rec)
}

// An outcome the store refuses is retried, and until it is stored the task
// reads as running: no terminal state is reported that the store does not hold.
func TestTaskStore_UnsavedOutcomeIsRetriedNotReported(t *testing.T) {
	store := &flakyTerminalStore{TaskStore: NewMemoryTaskStore(nil)}
	store.failures.Store(3)
	s := durableServer(store)
	s.taskStoreRetryMin, s.taskStoreRetryMax = 20*time.Millisecond, 40*time.Millisecond
	s.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("kept"), nil
	})
	id := startLegacyTask(t, s, "work")
	require.Eventually(t, func() bool { return store.attempts.Load() >= 1 }, 5*time.Second, time.Millisecond)
	task, _, err := s.getTask(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, mcp.TaskStatusWorking, task.Status, "not reported before it is stored")

	done := waitLocalTerminal(t, s, id)
	assert.Equal(t, mcp.TaskStatusCompleted, done.Status, "the original result, once stored")
	assert.GreaterOrEqual(t, store.attempts.Load(), int32(4))
	rec, _ := store.Get(context.Background(), id)
	assert.Equal(t, mcp.TaskStatusCompleted, rec.Task.Status)
	res, reqErr := s.handleTaskResult(t.Context(), 1, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: id}})
	require.Nil(t, reqErr)
	assert.Equal(t, "kept", res.Content[0].(mcp.TextContent).Text)
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

// tasks/result on another server's task rides out a transient store error.
func TestTaskStore_ResultWaitToleratesTransientErrors(t *testing.T) {
	inner := NewMemoryTaskStore(nil)
	a := durableServer(inner)
	release := make(chan struct{})
	a.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		<-release
		return mcp.NewToolResultText("ok"), nil
	})
	id := startLegacyTask(t, a, "work")
	store := &brokenGetStore{TaskStore: inner}
	b := durableServer(store)
	_, _, err := b.getTask(t.Context(), id) // warm: reads fine
	require.NoError(t, err)
	store.fails.Store(remoteTaskPollErrors - 1)
	go func() { time.Sleep(50 * time.Millisecond); close(release) }()
	// The first Gets fail (lookup included), so retry the lookup like a client would.
	var res *mcp.TaskResultResult
	require.Eventually(t, func() bool {
		r, reqErr := b.handleTaskResult(t.Context(), 1, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: id}})
		res = r
		return reqErr == nil
	}, 10*time.Second, 10*time.Millisecond)
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
	assert.Len(t, report.Resumed, 1)
	assert.Len(t, report.OverLimit, 1)
	assert.ElementsMatch(t, []string{one, two}, append(report.Resumed, report.OverLimit...))
	left, _ := store.Get(context.Background(), report.OverLimit[0])
	assert.Equal(t, mcp.TaskStatusWorking, left.Task.Status, "a task over the limit is left for later")
	assert.Equal(t, uint64(1), left.Revision, "and not claimed")
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
