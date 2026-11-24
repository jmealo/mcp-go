package server

// Authorization-hook tests (WithTaskAuthorizer / WithTaskOwner). The red
// cases run against the pre-hook behaviour: without the option nothing
// changes; with it, a denial is indistinguishable from "task not found", and
// the check judges the request the server itself decoded.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// authzEnv wires a server whose tasks record the caller named by ownerFor
// and whose authorizer admits only that caller.
type authzEnv struct {
	srv       *MCPServer
	mu        sync.Mutex
	seenOps   []TaskOp
	seenOwner map[string]string // taskID -> OwnerID seen by the authorizer
}

type authzCallerKey struct{}

func callerCtx(ctx context.Context, caller string) context.Context {
	return context.WithValue(ctx, authzCallerKey{}, caller)
}

func callerFrom(ctx context.Context) string {
	caller, _ := ctx.Value(authzCallerKey{}).(string)
	return caller
}

func newAuthzEnv(t *testing.T) *authzEnv {
	t.Helper()
	env := &authzEnv{seenOwner: map[string]string{}}
	env.srv = NewMCPServer("authz-test", "1.0.0",
		WithTaskCapabilities(true, true, true),
		WithTaskOwner(callerFrom),
		WithTaskAuthorizer(func(ctx context.Context, op TaskOp, rec TaskRecord) error {
			env.mu.Lock()
			env.seenOps = append(env.seenOps, op)
			env.seenOwner[rec.Task.TaskId] = rec.OwnerID
			env.mu.Unlock()
			if rec.OwnerID != callerFrom(ctx) {
				return fmt.Errorf("task belongs to %q", rec.OwnerID)
			}
			return nil
		}),
	)
	env.srv.AddTool(
		mcp.NewTool("job", mcp.WithTaskSupport(mcp.TaskSupportOptional)),
		func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("done"), nil
		},
	)
	return env
}

// createAs creates a modern task on behalf of caller and returns its ID.
func (e *authzEnv) createAs(t *testing.T, caller string) string {
	t.Helper()
	ctx := callerCtx(buildModernContext(t.Context(), e.srv), caller)
	created, reqErr := e.srv.handleToolCall(ctx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "job"}})
	require.Nil(t, reqErr)
	task, ok := created.(*mcp.CreateTaskResult)
	require.True(t, ok, "got %T", created)
	return task.Task.TaskId
}

func (e *authzEnv) ctx(t *testing.T, caller string) context.Context {
	t.Helper()
	return callerCtx(buildModernContext(t.Context(), e.srv), caller)
}

func (e *authzEnv) ops() []TaskOp {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]TaskOp(nil), e.seenOps...)
}

// TestTaskAuthorizer_DenialLooksLikeNotFound is the core red/green: a caller
// denied by the authorizer gets byte-identical -32602 "task not found", the
// same as an unknown ID.
func TestTaskAuthorizer_DenialLooksLikeNotFound(t *testing.T) {
	env := newAuthzEnv(t)
	aliceTask := env.createAs(t, "alice")

	aliceCtx, bobCtx := env.ctx(t, "alice"), env.ctx(t, "bob")

	// Green: the owner reads its own task.
	_, reqErr := env.srv.handleGetTask(aliceCtx, 2, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: aliceTask}})
	require.Nil(t, reqErr)

	// Red (pre-hook this succeeded): another caller reads "task not found",
	// identical to the unknown-ID answer.
	denied, reqErr := env.srv.handleGetTask(bobCtx, 3, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: aliceTask}})
	require.NotNil(t, reqErr, "denied request must error")
	require.Nil(t, denied)
	assert.Equal(t, mcp.INVALID_PARAMS, reqErr.code)
	assert.Equal(t, "task not found", reqErr.err.Error())

	unknown, reqErr2 := env.srv.handleGetTask(bobCtx, 4, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: "no-such-task"}})
	require.Nil(t, unknown)
	require.NotNil(t, reqErr2)
	assert.Equal(t, reqErr.err.Error(), reqErr2.err.Error(), "denial and unknown must be indistinguishable")

	// update and cancel are gated the same way.
	_, updErr := env.srv.handleUpdateTask(bobCtx, 5, mcp.UpdateTaskRequest{
		Params: mcp.UpdateTaskParams{TaskId: aliceTask},
	})
	require.NotNil(t, updErr)
	assert.Equal(t, "task not found", updErr.err.Error())

	_, cancelErr := env.srv.handleCancelTask(bobCtx, 7, mcp.CancelTaskRequest{
		Params: mcp.CancelTaskParams{TaskId: aliceTask},
	})
	require.NotNil(t, cancelErr)
	assert.Equal(t, "task not found", cancelErr.err.Error())
}

// TestTaskAuthorizer_RunsOnDecodedRequest proves the check survives payloads
// a stricter external parser would reject: extra params with unexpected
// types decode fine in the SDK, and the authorizer still gates the result.
// This is the parser-differential class that bypassed gisual-mcp's
// middleware.
func TestTaskAuthorizer_RunsOnDecodedRequest(t *testing.T) {
	env := newAuthzEnv(t)
	aliceTask := env.createAs(t, "alice")

	// Malformed-but-decodable: extra members with wrong types alongside the
	// real taskId, sent through HandleMessage like a transport would.
	meta, err := json.Marshal(modernTaskMeta())
	require.NoError(t, err)
	request := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tasks/get","params":{"taskId":%q,"name":0,"arguments":"not-an-object","_meta":%s}}`, aliceTask, meta)

	seen := make(chan struct{}, 1)
	env.srv.taskAuthorizer = func(ctx context.Context, op TaskOp, rec TaskRecord) error {
		select {
		case seen <- struct{}{}:
		default:
		}
		return fmt.Errorf("deny")
	}

	resp := env.srv.HandleMessage(env.ctx(t, "bob"), []byte(request))
	errResp, ok := resp.(mcp.JSONRPCError)
	require.True(t, ok, "got %T", resp)
	assert.Equal(t, mcp.INVALID_PARAMS, errResp.Error.Code)
	assert.Equal(t, "task not found", errResp.Error.Message)

	select {
	case <-seen:
	case <-time.After(time.Second):
		t.Fatal("authorizer was not consulted for the decoded request")
	}
}

// TestTaskAuthorizer_AllOpsConsulted verifies each operation reaches the
// hook with its TaskOp.
func TestTaskAuthorizer_AllOpsConsulted(t *testing.T) {
	env := newAuthzEnv(t)
	aliceCtx := env.ctx(t, "alice")

	// Legacy-era task for tasks/result coverage.
	legacyTask := "legacy-authz"
	sCtx := t.Context()
	entry, err := env.srv.createTask(sCtx, legacyTask, "job", nil, nil)
	require.NoError(t, err)
	env.srv.completeTask(entry, mcp.NewToolResultText("late"), nil)

	taskID := env.createAs(t, "alice")

	_, rErr := env.srv.handleGetTask(aliceCtx, 1, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
	require.Nil(t, rErr)
	_, rErr = env.srv.handleUpdateTask(aliceCtx, 2, mcp.UpdateTaskRequest{Params: mcp.UpdateTaskParams{TaskId: taskID}})
	require.Nil(t, rErr)
	_, rErr = env.srv.handleCancelTask(aliceCtx, 3, mcp.CancelTaskRequest{Params: mcp.CancelTaskParams{TaskId: taskID}})
	require.Nil(t, rErr)
	legacyCtx := t.Context()
	_, rErr = env.srv.handleTaskResult(legacyCtx, 4, mcp.TaskResultRequest{Params: mcp.TaskResultParams{TaskId: legacyTask}})
	require.Nil(t, rErr)

	assert.Contains(t, env.ops(), TaskOpGet)
	assert.Contains(t, env.ops(), TaskOpUpdate)
	assert.Contains(t, env.ops(), TaskOpCancel)
	assert.Contains(t, env.ops(), TaskOpResult)
}

// TestTaskOwner_PersistedThroughStore verifies OwnerID survives the
// TaskStore round trip, so authorizers compare the creating caller even for
// records loaded from storage.
func TestTaskOwner_PersistedThroughStore(t *testing.T) {
	env := &authzEnv{seenOwner: map[string]string{}}
	env.srv = NewMCPServer("authz-store-test", "1.0.0",
		WithTaskCapabilities(true, true, true),
		WithTaskOwner(callerFrom),
		WithTaskAuthorizer(func(ctx context.Context, op TaskOp, rec TaskRecord) error {
			env.mu.Lock()
			env.seenOwner[rec.Task.TaskId] = rec.OwnerID
			env.mu.Unlock()
			return nil
		}),
		WithTaskStore(NewMemoryTaskStore(time.Now)),
	)
	env.srv.AddTool(
		mcp.NewTool("job", mcp.WithTaskSupport(mcp.TaskSupportOptional)),
		func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("done"), nil
		},
	)

	aliceCtx := callerCtx(buildModernContext(t.Context(), env.srv), "alice")
	created, reqErr := env.srv.handleToolCall(aliceCtx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "job"}})
	require.Nil(t, reqErr)
	taskID := created.(*mcp.CreateTaskResult).Task.TaskId

	// Read the record straight from the store: OwnerID persisted.
	rec, err := env.srv.taskStore.Get(t.Context(), taskID)
	require.NoError(t, err)
	assert.Equal(t, "alice", rec.OwnerID)
}
