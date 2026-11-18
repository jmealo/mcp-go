package server

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mark3labs/mcp-go/mcp"
)

func durableServer(store TaskStore) *MCPServer {
	return NewMCPServer("t", "1", WithTaskCapabilities(true, true, true), WithTaskStore(store))
}

func startLegacyTask(t *testing.T, s *MCPServer, tool string) string {
	t.Helper()
	ttl := int64(60_000)
	res, reqErr := s.handleToolCall(t.Context(), 1, mcp.CallToolRequest{
		Params: mcp.CallToolParams{Name: tool, Arguments: map[string]any{"n": 1}, Task: &mcp.TaskParams{TTL: &ttl}},
	})
	require.Nil(t, reqErr)
	created, ok := res.(*mcp.CreateTaskResult)
	require.True(t, ok, "want a CreateTaskResult, got %T", res)
	return created.Task.TaskId
}

func taskTool(name string) mcp.Tool {
	return mcp.NewTool(name, mcp.WithTaskSupport(mcp.TaskSupportOptional))
}

func waitStored(t *testing.T, store TaskStore, id string, ok func(TaskRecord) bool) TaskRecord {
	t.Helper()
	var rec TaskRecord
	require.Eventually(t, func() bool {
		r, err := store.Get(context.Background(), id)
		if err != nil {
			return false
		}
		rec = r
		return ok(r)
	}, 5*time.Second, 5*time.Millisecond)
	return rec
}

func waitLocalTerminal(t *testing.T, s *MCPServer, id string) mcp.Task {
	t.Helper()
	var task mcp.Task
	require.Eventually(t, func() bool {
		tk, _, err := s.getTask(context.Background(), id)
		task = tk
		return err == nil && tk.Status.IsTerminal()
	}, 5*time.Second, 5*time.Millisecond)
	return task
}

// Server A runs a task that saves resume state and then hangs ("crashes").
// Server B shares the store, resumes it, finishes it, and serves tasks/get
// and tasks/result for it under the same ID. A's late result is rejected.
func TestTaskStore_RestartResumesTask(t *testing.T) {
	store := NewMemoryTaskStore(nil)

	a := durableServer(store)
	releaseA := make(chan struct{})
	aDone := make(chan struct{})
	a.AddTool(taskTool("work"), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		defer close(aDone)
		require.NoError(t, SetTaskResumeState(ctx, json.RawMessage(`{"cursor":"c-7"}`)))
		<-releaseA
		return mcp.NewToolResultText("late result from A"), nil
	})
	id := startLegacyTask(t, a, "work")
	waitStored(t, store, id, func(r TaskRecord) bool { return string(r.ResumeState) == `{"cursor":"c-7"}` })

	b := durableServer(store)
	var seenState json.RawMessage
	var seenID string
	var seenArgs any
	b.AddTool(taskTool("work"), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		seenState = TaskResumeStateFromContext(ctx)
		seenID, _ = TaskIDFromContext(ctx)
		seenArgs = req.GetArguments()["n"]
		return mcp.NewToolResultText("resumed from " + string(seenState)), nil
	})
	report, err := b.ResumeTasks(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{id}, report.Resumed)

	task := waitLocalTerminal(t, b, id)
	assert.Equal(t, mcp.TaskStatusCompleted, task.Status)
	assert.Equal(t, id, seenID)
	assert.JSONEq(t, `{"cursor":"c-7"}`, string(seenState))
	assert.EqualValues(t, 1, seenArgs, "the original request is re-run")

	res, reqErr := b.handleTaskResult(t.Context(), 2, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: id}})
	require.Nil(t, reqErr)
	require.Len(t, res.Content, 1)
	assert.Equal(t, `resumed from {"cursor":"c-7"}`, res.Content[0].(mcp.TextContent).Text)

	// A finishes late: its completion must not overwrite B's stored result.
	close(releaseA)
	<-aDone
	require.Eventually(t, func() bool {
		tk, _, err := a.getTask(context.Background(), id)
		return err == nil && tk.Status.IsTerminal()
	}, 5*time.Second, 5*time.Millisecond)
	rec, err := store.Get(context.Background(), id)
	require.NoError(t, err)
	assert.Contains(t, string(rec.Result), "resumed from", "the first stored outcome wins")
	aRes, reqErr := a.handleTaskResult(t.Context(), 3, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: id}})
	require.Nil(t, reqErr)
	assert.Contains(t, aRes.Content[0].(mcp.TextContent).Text, "resumed from", "A adopts the stored outcome")
}

// A task already ended is not resumed: completed, failed or cancelled.
func TestTaskStore_EndedTasksAreNotResumed(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	a := durableServer(store)
	block := make(chan struct{})
	defer close(block)
	a.AddTool(taskTool("quick"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("ok"), nil
	})
	a.AddTool(taskTool("fail"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return nil, errors.New("boom")
	})
	a.AddTool(taskTool("slow"), func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return nil, ctx.Err()
	})
	done := startLegacyTask(t, a, "quick")
	failed := startLegacyTask(t, a, "fail")
	cancelled := startLegacyTask(t, a, "slow")
	require.NoError(t, a.cancelTask(t.Context(), cancelled, false))
	for _, id := range []string{done, failed, cancelled} {
		waitStored(t, store, id, func(r TaskRecord) bool { return r.Task.Status.IsTerminal() })
	}
	rec, _ := store.Get(context.Background(), cancelled)
	assert.Equal(t, mcp.TaskStatusCancelled, rec.Task.Status)
	require.NotNil(t, rec.Error)
	assert.True(t, rec.Error.Cancelled)

	b := durableServer(store)
	ran := false
	for _, n := range []string{"quick", "fail", "slow"} {
		b.AddTool(taskTool(n), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			ran = true
			return mcp.NewToolResultText("again"), nil
		})
	}
	report, err := b.ResumeTasks(t.Context())
	require.NoError(t, err)
	assert.Empty(t, report.Resumed)
	assert.False(t, ran, "no ended task's handler runs again")

	// B still serves the ended tasks from the store.
	res, reqErr := b.handleTaskResult(t.Context(), 1, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: done}})
	require.Nil(t, reqErr)
	assert.Equal(t, "ok", res.Content[0].(mcp.TextContent).Text)
	_, reqErr = b.handleTaskResult(t.Context(), 2, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: cancelled}})
	require.NotNil(t, reqErr)
	assert.Equal(t, mcp.REQUEST_INTERRUPTED, reqErr.code)
	_, reqErr = b.handleTaskResult(t.Context(), 3, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: failed}})
	require.NotNil(t, reqErr)
	assert.Contains(t, reqErr.err.Error(), "boom")
}

func TestTaskStore_ResumeFailsWhatCannotRun(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	ctx := context.Background()
	gone := record(t, store, "gone", "missing_tool", mcp.TaskStatusWorking, `{"method":"tools/call","params":{"name":"missing_tool"}}`)
	waiting := record(t, store, "waiting", "work", mcp.TaskStatusInputRequired, `{"method":"tools/call","params":{"name":"work"}}`)
	broken := record(t, store, "broken", "work", mcp.TaskStatusWorking, `not json`)

	b := durableServer(store)
	b.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		t.Error("a task that cannot resume must not run")
		return nil, nil
	})
	report, err := b.ResumeTasks(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{gone, waiting, broken}, report.Failed)
	for id, msg := range map[string]string{gone: "no longer registered", waiting: "awaiting client input", broken: "cannot be resumed"} {
		rec, err := store.Get(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, mcp.TaskStatusFailed, rec.Task.Status, id)
		assert.Contains(t, rec.Task.StatusMessage, msg, id)
	}
}

func record(t *testing.T, store TaskStore, id, tool string, status mcp.TaskStatus, request string) string {
	t.Helper()
	task := mcp.NewTask(id)
	task.Status = status
	_, err := store.Create(context.Background(), TaskRecord{Task: task, ToolName: tool, CreatedAt: time.Now(), Request: json.RawMessage(request)})
	require.NoError(t, err)
	return id
}

// Server B reads and cancels a task that server A runs; A's handler is
// stopped at its next store write.
func TestTaskStore_CancelFromAnotherServer(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	a := durableServer(store)
	tick := make(chan struct{})
	aErr := make(chan error, 1)
	a.AddTool(taskTool("work"), func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		<-tick
		err := SetTaskResumeState(ctx, json.RawMessage(`2`))
		aErr <- err
		<-ctx.Done()
		return nil, ctx.Err()
	})
	id := startLegacyTask(t, a, "work")

	b := durableServer(store)
	task, _, err := b.getTask(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, mcp.TaskStatusWorking, task.Status, "B reads A's running task from the store")
	require.NoError(t, b.cancelTask(t.Context(), id, false))
	rec, _ := store.Get(context.Background(), id)
	assert.Equal(t, mcp.TaskStatusCancelled, rec.Task.Status)

	close(tick)
	assert.ErrorIs(t, <-aErr, ErrTaskTerminal)
	got := waitLocalTerminal(t, a, id)
	assert.Equal(t, mcp.TaskStatusCancelled, got.Status, "A adopts the stored cancellation and stops")
}

// tasks/result on B waits for a task A is still running.
func TestTaskStore_ResultWaitsForAnotherServer(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	a := durableServer(store)
	release := make(chan struct{})
	a.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		<-release
		return mcp.NewToolResultText("from A"), nil
	})
	id := startLegacyTask(t, a, "work")
	b := durableServer(store)
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()
	res, reqErr := b.handleTaskResult(t.Context(), 1, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: id}})
	require.Nil(t, reqErr)
	assert.Equal(t, "from A", res.Content[0].(mcp.TextContent).Text)
}

// failingStore wraps a store and fails chosen operations.
type failingStore struct {
	TaskStore
	mu           sync.Mutex
	failCreate   bool
	failTerminal mcp.TaskStatus // Update to this status fails
	failures     int
}

func (f *failingStore) Create(ctx context.Context, rec TaskRecord) (TaskRecord, error) {
	if f.failCreate {
		return TaskRecord{}, errors.New("disk full")
	}
	return f.TaskStore.Create(ctx, rec)
}

func (f *failingStore) Update(ctx context.Context, rec TaskRecord) (TaskRecord, error) {
	if f.failTerminal != "" && rec.Task.Status == f.failTerminal {
		f.mu.Lock()
		f.failures++
		f.mu.Unlock()
		return TaskRecord{}, errors.New("disk full")
	}
	return f.TaskStore.Update(ctx, rec)
}

func TestTaskStore_CreateFailureIsAnInternalError(t *testing.T) {
	s := durableServer(&failingStore{TaskStore: NewMemoryTaskStore(nil), failCreate: true})
	ran := false
	s.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ran = true
		return mcp.NewToolResultText("x"), nil
	})
	_, reqErr := s.handleToolCall(t.Context(), 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "work", Task: &mcp.TaskParams{}}})
	require.NotNil(t, reqErr)
	assert.Equal(t, mcp.INTERNAL_ERROR, reqErr.code)
	assert.Contains(t, reqErr.err.Error(), "disk full")
	assert.False(t, ran)
	assert.Empty(t, s.listTasks(t.Context()), "no task is left behind")
	s.tasksMu.RLock()
	defer s.tasksMu.RUnlock()
	assert.Zero(t, s.activeTasks)
	assert.Empty(t, s.tasks)
}

// A result the store cannot save is not reported: the task fails instead.
func TestTaskStore_UnsavedResultFailsTheTask(t *testing.T) {
	store := &failingStore{TaskStore: NewMemoryTaskStore(nil), failTerminal: mcp.TaskStatusCompleted}
	s := durableServer(store)
	s.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("unsaved"), nil
	})
	id := startLegacyTask(t, s, "work")
	task := waitLocalTerminal(t, s, id)
	assert.Equal(t, mcp.TaskStatusFailed, task.Status)
	assert.Contains(t, task.StatusMessage, "could not be stored")
	rec := waitStored(t, store, id, func(r TaskRecord) bool { return r.Task.Status.IsTerminal() })
	assert.Equal(t, mcp.TaskStatusFailed, rec.Task.Status, "the stored state matches what is reported")
}

// Without a store, nothing about tasks changes (the default).
func TestTaskStore_DefaultIsInProcess(t *testing.T) {
	s := NewMCPServer("t", "1", WithTaskCapabilities(true, true, true))
	assert.Nil(t, s.taskStore)
	report, err := s.ResumeTasks(t.Context())
	require.NoError(t, err)
	assert.Empty(t, report.Resumed)
	assert.ErrorIs(t, SetTaskResumeState(t.Context(), nil), ErrNotInTask)
	_, ok := TaskIDFromContext(t.Context())
	assert.False(t, ok)
}

// An expired task is deleted from the store with its local entry.
func TestTaskStore_ExpiryDeletesRecord(t *testing.T) {
	store := NewMemoryTaskStore(nil)
	s := durableServer(store)
	s.AddTool(taskTool("work"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("x"), nil
	})
	ttl := int64(30)
	res, reqErr := s.handleToolCall(t.Context(), 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "work", Task: &mcp.TaskParams{TTL: &ttl}}})
	require.Nil(t, reqErr)
	id := res.(*mcp.CreateTaskResult).Task.TaskId
	require.Eventually(t, func() bool {
		_, err := store.Get(context.Background(), id)
		return errors.Is(err, ErrTaskNotFound)
	}, 5*time.Second, 5*time.Millisecond)
	_, _, err := s.getTask(t.Context(), id)
	assert.ErrorContains(t, err, "expired")
}
