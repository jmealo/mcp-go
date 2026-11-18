package server

// Tests for the three SEP-2663 review fixes:
//  1. subscriptions/listen with taskIds from a client that did not declare
//     the tasks extension must fail with -32021 naming the extension.
//  2. An inputRequests key re-issued in a later round of the same task fails
//     the task (keys are unique over a task's lifetime).
//  3. tasks/update for an existing task that is not awaiting input acks
//     idempotently instead of erroring.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func modernTasksCtx(ctx context.Context) context.Context {
	return WithRequestProtocolInfo(ctx, &RequestProtocolInfo{
		Modern:          true,
		ProtocolVersion: mcp.ProtocolVersion20260728,
		ClientCapabilities: &mcp.ClientCapabilities{
			Extensions: map[string]any{mcp.ExtensionTasks: map[string]any{}},
		},
	})
}

func modernNoTasksCtx(ctx context.Context) context.Context {
	return WithRequestProtocolInfo(ctx, &RequestProtocolInfo{
		Modern:          true,
		ProtocolVersion: mcp.ProtocolVersion20260728,
	})
}

// TestSubscriptionsListen_TaskIdsRequireExtension verifies the -32021 gate.
func TestSubscriptionsListen_TaskIdsRequireExtension(t *testing.T) {
	srv := NewMCPServer("gate-test", "1.0.0", WithToolCapabilities(true))
	req := mcp.SubscriptionsListenRequest{
		Params: mcp.SubscriptionsListenParams{
			Notifications: mcp.SubscriptionFilter{TaskIds: []string{"some-task"}},
		},
	}

	// Non-declaring client: -32021 with requiredCapabilities data.
	_, reqErr := srv.handleSubscriptionsListen(modernNoTasksCtx(t.Context()), 1, req)
	require.NotNil(t, reqErr)
	require.Equal(t, mcp.MISSING_REQUIRED_CLIENT_CAPABILITY, reqErr.code)
	wire, err := json.Marshal(reqErr.ToJSONRPCError())
	require.NoError(t, err)
	assert.Contains(t, string(wire), "requiredCapabilities")
	assert.Contains(t, string(wire), mcp.ExtensionTasks)

	// A client that did not ask for task notifications is unaffected. The
	// filter asks for prompts the server does not offer, so nothing is
	// established and the handler returns instead of holding the stream.
	plainSrv := NewMCPServer("gate-test-plain", "1.0.0", WithToolCapabilities(true))
	session := newListenSession()
	require.NoError(t, plainSrv.RegisterSession(t.Context(), session))
	ctx := plainSrv.WithContext(modernNoTasksCtx(t.Context()), session)
	_, reqErr = plainSrv.handleSubscriptionsListen(ctx, 2, mcp.SubscriptionsListenRequest{
		Params: mcp.SubscriptionsListenParams{
			Notifications: mcp.SubscriptionFilter{PromptsListChanged: true},
		},
	})
	require.Nil(t, reqErr, "taskIds gate must not affect other filter fields")

	// Declaring client: the request succeeds. The server does not push
	// notifications/tasks, so the acknowledged filter must not echo taskIds
	// and the stream closes immediately (nothing established).
	declaringSrv := NewMCPServer("gate-test-declaring", "1.0.0", WithToolCapabilities(true))
	declaringSession := newListenSession()
	require.NoError(t, declaringSrv.RegisterSession(t.Context(), declaringSession))
	declaringCtx := declaringSrv.WithContext(modernTasksCtx(t.Context()), declaringSession)
	result, reqErr := declaringSrv.handleSubscriptionsListen(declaringCtx, 3, req)
	require.Nil(t, reqErr)
	require.NotNil(t, result)

	select {
	case ack := <-declaringSession.notify:
		require.Equal(t, mcp.MethodNotificationSubscriptionsAcknowledged, ack.Method)
		notifications, ok := ack.Params.AdditionalFields["notifications"].(mcp.SubscriptionFilter)
		require.True(t, ok, "ack params carry the notifications filter")
		assert.Empty(t, notifications.TaskIds,
			"a server that pushes no notifications/tasks must not agree to taskIds")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscriptions/acknowledged")
	}
}

// inputRoundTool elicits the given keys, one per round, using the MRTR
// requestState to track rounds: each retry echoes the state of the round it
// answered, exactly as the synchronous multi round-trip flow does.
func inputRoundTool(srv *MCPServer, name string, keys []string) {
	stateFor := func(round int) string { return fmt.Sprintf("round-%d", round) }
	srv.AddTool(
		mcp.NewTool(name, mcp.WithTaskSupport(mcp.TaskSupportOptional)),
		func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			round := 0
			for i := range keys {
				if req.Params.RequestState == stateFor(i) {
					round = i + 1
				}
			}
			if round >= len(keys) {
				return mcp.NewToolResultText("done"), nil
			}
			key := keys[round]
			return NewInputRequestBuilder(stateFor(round)).
				Elicit(key, mcp.ElicitationParams{
					Mode:            mcp.ElicitationModeForm,
					Message:         "Round " + key,
					RequestedSchema: map[string]any{"type": "object"},
				}).ToolResult(), nil
		})
}

func answerFor(key string) mcp.InputResponses {
	return mcp.InputResponses{key: mcp.NewElicitationInputResponse(mcp.ElicitationResult{
		ElicitationResponse: mcp.ElicitationResponse{Action: mcp.ElicitationResponseActionAccept},
	})}
}

// TestAwaitTaskInput_KeyReuseFailsTask verifies that a key re-issued in a
// later round of the same task fails the task instead of confusing clients
// that deduplicate by key.
func TestAwaitTaskInput_KeyReuseFailsTask(t *testing.T) {
	srv := NewMCPServer("key-test", "1.0.0", WithTaskCapabilities(true, true, true))
	inputRoundTool(srv, "flaky", []string{"confirm", "confirm"}) // same key twice

	ctx := modernTasksCtx(t.Context())
	created, reqErr := srv.handleToolCall(ctx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "flaky"}})
	require.Nil(t, reqErr)
	task, ok := created.(*mcp.CreateTaskResult)
	require.True(t, ok, "got %T", created)
	taskID := task.Task.TaskId

	// Answer round one; the handler then re-elicits with the same key, which
	// must fail the task.
	require.Eventually(t, func() bool {
		got, e := srv.handleGetTask(ctx, 2, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
		return e == nil && got.Status == mcp.TaskStatusInputRequired
	}, 5*time.Second, 10*time.Millisecond)

	_, reqErr = srv.handleUpdateTask(ctx, 3, mcp.UpdateTaskRequest{
		Params: mcp.UpdateTaskParams{TaskId: taskID, InputResponses: answerFor("confirm")},
	})
	require.Nil(t, reqErr)

	require.Eventually(t, func() bool {
		got, e := srv.handleGetTask(ctx, 4, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
		return e == nil && got.Status == mcp.TaskStatusFailed
	}, 5*time.Second, 10*time.Millisecond)

	got, reqErr := srv.handleGetTask(ctx, 4, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
	require.Nil(t, reqErr)
	require.NotNil(t, got.TaskError)
	assert.Contains(t, got.TaskError.Message, `"confirm"`, "error names the re-issued key")
}

// TestAwaitTaskInput_DistinctKeysAcrossRoundsSucceed is the control: a second
// round with a fresh key completes normally.
func TestAwaitTaskInput_DistinctKeysAcrossRoundsSucceed(t *testing.T) {
	srv := NewMCPServer("key-test", "1.0.0", WithTaskCapabilities(true, true, true))
	inputRoundTool(srv, "tworound", []string{"q1", "q2"})

	ctx := modernTasksCtx(t.Context())
	created, reqErr := srv.handleToolCall(ctx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "tworound"}})
	require.Nil(t, reqErr)
	task, ok := created.(*mcp.CreateTaskResult)
	require.True(t, ok, "got %T", created)
	taskID := task.Task.TaskId

	for i, key := range []string{"q1", "q2"} {
		require.Eventually(t, func() bool {
			got, e := srv.handleGetTask(ctx, 2, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
			if e != nil {
				return false
			}
			// Wait for this round's request: right after the previous answer the
			// task is still input_required with an empty request map.
			_, asked := got.InputRequests[key]
			return got.Status == mcp.TaskStatusInputRequired && asked
		}, 5*time.Second, 10*time.Millisecond, "waiting to answer %s", key)

		got, reqErr := srv.handleGetTask(ctx, 2, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
		require.Nil(t, reqErr)
		require.Contains(t, got.InputRequests, key)

		_, reqErr = srv.handleUpdateTask(ctx, 10+i, mcp.UpdateTaskRequest{
			Params: mcp.UpdateTaskParams{TaskId: taskID, InputResponses: answerFor(key)},
		})
		require.Nil(t, reqErr)
	}

	require.Eventually(t, func() bool {
		got, e := srv.handleGetTask(ctx, 5, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
		return e == nil && got.Status == mcp.TaskStatusCompleted
	}, 5*time.Second, 10*time.Millisecond)

	got, reqErr := srv.handleGetTask(ctx, 5, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
	require.Nil(t, reqErr)
	assert.NotNil(t, got.TaskResult)
}

// TestUpdateTask_AcksWhenNotAwaitingInput verifies the idempotent ack: an
// update for a task in working or terminal status succeeds.
func TestUpdateTask_AcksWhenNotAwaitingInput(t *testing.T) {
	srv := NewMCPServer("ack-test", "1.0.0", WithTaskCapabilities(true, true, true))
	block := make(chan struct{})
	srv.AddTool(
		mcp.NewTool("slow", mcp.WithTaskSupport(mcp.TaskSupportOptional)),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			select {
			case <-block:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return mcp.NewToolResultText("done"), nil
		})

	ctx := modernTasksCtx(t.Context())
	created, reqErr := srv.handleToolCall(ctx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "slow"}})
	require.Nil(t, reqErr)
	task, ok := created.(*mcp.CreateTaskResult)
	require.True(t, ok, "got %T", created)
	taskID := task.Task.TaskId

	// Working status: ack, not INVALID_PARAMS.
	require.Eventually(t, func() bool {
		got, e := srv.handleGetTask(ctx, 2, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
		return e == nil && got.Status == mcp.TaskStatusWorking
	}, 5*time.Second, 10*time.Millisecond)

	result, reqErr := srv.handleUpdateTask(ctx, 3, mcp.UpdateTaskRequest{
		Params: mcp.UpdateTaskParams{TaskId: taskID, InputResponses: answerFor("anything")},
	})
	require.Nil(t, reqErr, "update on a working task must ack")
	require.NotNil(t, result)
	assert.Equal(t, mcp.ResultTypeComplete, result.GetResultType())

	// Terminal status: still acks.
	close(block)
	require.Eventually(t, func() bool {
		got, e := srv.handleGetTask(ctx, 4, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
		return e == nil && got.Status == mcp.TaskStatusCompleted
	}, 5*time.Second, 10*time.Millisecond)

	result, reqErr = srv.handleUpdateTask(ctx, 5, mcp.UpdateTaskRequest{
		Params: mcp.UpdateTaskParams{TaskId: taskID, InputResponses: answerFor("anything")},
	})
	require.Nil(t, reqErr, "update on a completed task must ack")
	assert.Equal(t, mcp.ResultTypeComplete, result.GetResultType())

	// Unknown task still errors (SHOULD per spec).
	_, reqErr = srv.handleUpdateTask(ctx, 6, mcp.UpdateTaskRequest{
		Params: mcp.UpdateTaskParams{TaskId: "does-not-exist"},
	})
	require.NotNil(t, reqErr)
	assert.Equal(t, mcp.INVALID_PARAMS, reqErr.code)
}
