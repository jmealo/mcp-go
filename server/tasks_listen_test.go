package server

// subscriptions/listen taskIds behaviour: unknown and unauthorized IDs are
// dropped from the ack, the stream ends when every acknowledged task is
// terminal with its final notification delivered (or expired), and the
// per-caller cap and lifetime bounds hold.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listenTaskResult is what a direct handleSubscriptionsListen call yields.
type listenTaskResult struct {
	result *mcp.SubscriptionsListenResult
	err    *requestError
}

// startTaskListen runs subscriptions/listen for taskIDs on a registered
// session, waits for the ack, and returns the session, the acked filter,
// and a channel yielding the handler's return. cancel ends the stream.
func startTaskListen(t *testing.T, srv *MCPServer, caller string, taskIDs []string) (
	session *listenSession, acked mcp.SubscriptionFilter, returned chan listenTaskResult, cancel context.CancelFunc) {
	t.Helper()

	session = newListenSession()
	ctx, cancel := context.WithCancel(srv.WithContext(t.Context(), session))
	ctx = WithRequestProtocolInfo(ctx, &RequestProtocolInfo{
		Modern:          true,
		ProtocolVersion: mcp.ProtocolVersion20260728,
		ClientCapabilities: &mcp.ClientCapabilities{
			Extensions: map[string]any{mcp.ExtensionTasks: map[string]any{}},
		},
	})
	if caller != "" {
		ctx = callerCtx(ctx, caller)
	}

	returned = make(chan listenTaskResult, 1)
	go func() {
		result, err := srv.handleSubscriptionsListen(ctx, 31, mcp.SubscriptionsListenRequest{
			Params: mcp.SubscriptionsListenParams{
				Notifications: mcp.SubscriptionFilter{TaskIds: taskIDs},
			},
		})
		returned <- listenTaskResult{result, err}
	}()

	select {
	case ack := <-session.notify:
		require.Equal(t, mcp.MethodNotificationSubscriptionsAcknowledged, ack.Method)
		filter, ok := ack.Params.AdditionalFields["notifications"].(mcp.SubscriptionFilter)
		require.True(t, ok)
		acked = filter
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscriptions/acknowledged")
	}
	return session, acked, returned, cancel
}

// TestTaskListen_DropsUnknownAndUnauthorizedIDs verifies the acknowledged
// filter is narrowed to IDs the caller may observe.
func TestTaskListen_DropsUnknownAndUnauthorizedIDs(t *testing.T) {
	env := newAuthzEnv(t)
	aliceTask := env.createAs(t, "alice")

	_, acked, returned, cancel := startTaskListen(t, env.srv, "alice",
		[]string{aliceTask, "no-such-task"})
	defer cancel()
	assert.Equal(t, []string{aliceTask}, acked.TaskIds,
		"unknown IDs are dropped from the ack")

	// The authorizer denies bob every task but his own; his stream for
	// alice's task acknowledges nothing and closes immediately.
	_, ackedBob, returnedBob, cancelBob := startTaskListen(t, env.srv, "bob",
		[]string{aliceTask})
	defer cancelBob()
	assert.Empty(t, ackedBob.TaskIds, "unauthorized IDs are dropped from the ack")

	select {
	case r := <-returnedBob:
		require.Nil(t, r.err)
		require.NotNil(t, r.result, "task-only stream with empty ack returns at once")
	case <-time.After(2 * time.Second):
		t.Fatal("stream with nothing acknowledged did not close")
	}
	_ = returned
}

// TestTaskListen_StreamEndsWhenTasksFinish verifies the close rule: the
// stream returns by itself once every acknowledged task is terminal and its
// final notification was delivered - no client disconnect needed.
func TestTaskListen_StreamEndsWhenTasksFinish(t *testing.T) {
	env := newAuthzEnv(t)

	block := make(chan struct{})
	env.srv.AddTool(
		mcp.NewTool("slowjob", mcp.WithTaskSupport(mcp.TaskSupportOptional)),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			select {
			case <-block:
				return mcp.NewToolResultText("late"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	)
	aliceCtx := callerCtx(buildModernContext(t.Context(), env.srv), "alice")
	created, reqErr := env.srv.handleToolCall(aliceCtx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "slowjob"}})
	require.Nil(t, reqErr)
	taskID := created.(*mcp.CreateTaskResult).Task.TaskId

	session, acked, returned, cancel := startTaskListen(t, env.srv, "alice", []string{taskID})
	defer cancel()
	require.Equal(t, []string{taskID}, acked.TaskIds)

	// Finish the task; the terminal notification must arrive, then the
	// stream must close on its own.
	close(block)

	deadline := time.After(5 * time.Second)
	sawTerminal := false
	for !sawTerminal {
		select {
		case n := <-session.notify:
			require.Equal(t, mcp.MethodNotificationTasks, n.Method)
			var fields map[string]any
			require.NoError(t, json.Unmarshal([]byte(mustMarshalT(t, n.Params)), &fields))
			if fields["status"] == "completed" {
				sawTerminal = true
			}
		case <-deadline:
			t.Fatal("terminal notification never arrived")
		}
	}

	select {
	case r := <-returned:
		require.Nil(t, r.err)
		require.NotNil(t, r.result, "stream must close after the final notification")
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not close after every subscribed task finished")
	}
}

// TestTaskListen_PerCallerCap verifies WithMaxTaskListenStreams refuses a
// second concurrent stream for the same caller, and frees the slot after
// the first closes.
func TestTaskListen_PerCallerCap(t *testing.T) {
	env := &authzEnv{seenOwner: map[string]string{}}
	env.srv = NewMCPServer("cap-test", "1.0.0",
		WithTaskCapabilities(true, true, true),
		WithTaskOwner(callerFrom),
		WithMaxTaskListenStreams(1),
	)
	block := make(chan struct{})
	env.srv.AddTool(
		mcp.NewTool("job", mcp.WithTaskSupport(mcp.TaskSupportOptional)),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			<-block
			return mcp.NewToolResultText("done"), nil
		},
	)
	aliceCtx := callerCtx(buildModernContext(t.Context(), env.srv), "alice")
	created, reqErr := env.srv.handleToolCall(aliceCtx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "job"}})
	require.Nil(t, reqErr)
	taskID := created.(*mcp.CreateTaskResult).Task.TaskId

	_, acked, _, cancel1 := startTaskListen(t, env.srv, "alice", []string{taskID})
	defer cancel1()
	require.Equal(t, []string{taskID}, acked.TaskIds)

	// A second stream for the same caller is refused.
	session2 := newListenSession()
	ctx2, cancel2 := context.WithCancel(env.srv.WithContext(t.Context(), session2))
	ctx2 = WithRequestProtocolInfo(ctx2, &RequestProtocolInfo{
		Modern:          true,
		ProtocolVersion: mcp.ProtocolVersion20260728,
		ClientCapabilities: &mcp.ClientCapabilities{
			Extensions: map[string]any{mcp.ExtensionTasks: map[string]any{}},
		},
	})
	ctx2 = callerCtx(ctx2, "alice")
	_, err := env.srv.handleSubscriptionsListen(ctx2, 32, mcp.SubscriptionsListenRequest{
		Params: mcp.SubscriptionsListenParams{
			Notifications: mcp.SubscriptionFilter{TaskIds: []string{taskID}},
		},
	})
	cancel2()
	require.NotNil(t, err)
	assert.Equal(t, mcp.INVALID_PARAMS, err.code)
	assert.ErrorIs(t, err.err, errTooManyTaskListenStreams)

	// Another caller is unaffected.
	_, ackedBob, _, cancelBob := startTaskListen(t, env.srv, "bob", []string{taskID})
	defer cancelBob()
	assert.Equal(t, []string{taskID}, ackedBob.TaskIds)

	// Finishing the task closes alice's stream and frees her slot.
	close(block)
	select {
	case <-time.After(3 * time.Second):
	case <-ctx2.Done():
	}
}

// TestTaskListen_MaxLifetime verifies WithTaskListenMaxLifetime closes an
// otherwise-idle stream.
func TestTaskListen_MaxLifetime(t *testing.T) {
	env := &authzEnv{seenOwner: map[string]string{}}
	env.srv = NewMCPServer("lifetime-test", "1.0.0",
		WithTaskCapabilities(true, true, true),
		WithTaskOwner(callerFrom),
		WithTaskListenMaxLifetime(60*time.Millisecond),
	)
	block := make(chan struct{})
	defer close(block)
	env.srv.AddTool(
		mcp.NewTool("job", mcp.WithTaskSupport(mcp.TaskSupportOptional)),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			<-block
			return mcp.NewToolResultText("done"), nil
		},
	)
	aliceCtx := callerCtx(buildModernContext(t.Context(), env.srv), "alice")
	created, reqErr := env.srv.handleToolCall(aliceCtx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "job"}})
	require.Nil(t, reqErr)
	taskID := created.(*mcp.CreateTaskResult).Task.TaskId

	_, _, returned, cancel := startTaskListen(t, env.srv, "alice", []string{taskID})
	defer cancel()

	select {
	case r := <-returned:
		require.Nil(t, r.err)
		require.NotNil(t, r.result, "stream must close when the lifetime elapses")
	case <-time.After(3 * time.Second):
		t.Fatal("idle stream outlived its maximum lifetime")
	}
}

func mustMarshalT(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return string(raw)
}
