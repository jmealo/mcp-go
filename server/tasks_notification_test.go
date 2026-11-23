package server

// Tests for notifications/tasks push delivery (SEP-2663): subscriptions
// established via subscriptions/listen taskIds receive the complete
// DetailedTask on every status transition, while everyone else is left to
// poll.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mark3labs/mcp-go/mcp"
)

// subscribeToTask runs subscriptions/listen for the given task IDs on a
// registered session and returns the session plus a stop function. The
// request declares the tasks extension, as the gate requires.
func subscribeToTask(t *testing.T, srv *MCPServer, taskIDs ...string) (*listenSession, func()) {
	t.Helper()

	session := newListenSession()
	require.NoError(t, srv.RegisterSession(t.Context(), session))

	ctx, cancel := context.WithCancel(srv.WithContext(t.Context(), session))
	ctx = WithRequestProtocolInfo(ctx, &RequestProtocolInfo{
		Modern:          true,
		ProtocolVersion: mcp.ProtocolVersion20260728,
		ClientCapabilities: &mcp.ClientCapabilities{
			Extensions: map[string]any{mcp.ExtensionTasks: map[string]any{}},
		},
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = srv.handleSubscriptionsListen(ctx, 21, mcp.SubscriptionsListenRequest{
			Params: mcp.SubscriptionsListenParams{
				Notifications: mcp.SubscriptionFilter{TaskIds: taskIDs},
			},
		})
	}()

	// Wait for the acknowledgement so the filter is active before the task
	// transitions start firing.
	select {
	case ack := <-session.notify:
		require.Equal(t, mcp.MethodNotificationSubscriptionsAcknowledged, ack.Method)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscriptions/acknowledged")
	}

	return session, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("subscriptions/listen did not close")
		}
	}
}

// TestSubscriptionsListen_TaskIDsEchoedInAck verifies a task-running server
// agrees to the requested task subscriptions and echoes them in the ack.
func TestSubscriptionsListen_TaskIDsEchoedInAck(t *testing.T) {
	srv := NewMCPServer("push-test", "1.0.0", WithTaskCapabilities(true, true, true))

	session, stop := subscribeToTask(t, srv, "task-a", "task-b")
	defer stop()

	// Drain the ack (subscribeToTask already consumed it); nothing else may
	// arrive on a stream with no activity.
	select {
	case n := <-session.notify:
		t.Fatalf("unexpected notification %q on idle subscription", n.Method)
	default:
	}
}

// TestTaskNotifications_PushedToSubscribers drives a modern task through
// input_required to completion and verifies the subscribed session receives
// notifications/tasks with the status-specific payload inlined, while a
// registered non-subscriber receives nothing.
func TestTaskNotifications_PushedToSubscribers(t *testing.T) {
	srv := NewMCPServer("push-test", "1.0.0", WithTaskCapabilities(true, true, true))

	subscriber, stop := subscribeToTask(t, srv, "push-task")
	defer stop()

	// A session on a listen stream that never opted into this task must not
	// be told about it: the task ID is a bearer handle. Registered via the
	// pointer-keyed listen-session map to avoid the fixed test session ID.
	bystander := newListenSession()
	srv.registerListenSession(bystander)
	defer srv.unregisterListenSession(bystander)

	modernCtx := buildModernContext(t.Context(), srv)
	entry, err := srv.createTask(modernCtx, "push-task", "deploy", nil, nil)
	require.NoError(t, err)

	requests := mcp.InputRequests{
		"confirm": mcp.NewElicitationInputRequest(mcp.ElicitationParams{
			Mode:    mcp.ElicitationModeForm,
			Message: "Proceed?",
		}),
	}

	worker := make(chan struct{})
	go func() {
		defer close(worker)
		taskCtx, cancel := context.WithCancel(context.WithoutCancel(modernCtx))
		defer cancel()
		srv.tasksMu.Lock()
		entry.cancelFunc = cancel
		srv.tasksMu.Unlock()

		responses, ok := srv.awaitTaskInput(taskCtx, entry, requests,
			func(mcp.InputResponses) mcp.CallToolRequest { return mcp.CallToolRequest{} })
		if !ok {
			return
		}
		_, answered := responses["confirm"]
		assert.True(t, answered, "worker must receive the confirm response")
		srv.completeTask(entry, mcp.NewToolResultText("deployed"), nil)
	}()

	// input_required push, with the request inlined.
	var parked mcp.JSONRPCNotification
	require.Eventually(t, func() bool {
		select {
		case n := <-subscriber.notify:
			parked = n
			return true
		default:
			return false
		}
	}, 2*time.Second, 5*time.Millisecond)

	require.Equal(t, mcp.MethodNotificationTasks, parked.Method)
	var fields map[string]any
	require.NoError(t, json.Unmarshal([]byte(mustMarshal(t, parked.Params)), &fields))
	assert.Equal(t, "input_required", fields["status"])
	assert.NotContains(t, fields, "resultType", "notification params carry no result discriminator")
	inputRequests, ok := fields["inputRequests"].(map[string]any)
	require.True(t, ok, "input_required push inlines inputRequests, got %T", fields["inputRequests"])
	assert.Contains(t, inputRequests, "confirm")

	// Answer, resuming the worker: working then completed pushes follow.
	_, reqErr := srv.handleUpdateTask(modernCtx, 22, mcp.UpdateTaskRequest{
		Params: mcp.UpdateTaskParams{
			TaskId:         "push-task",
			InputResponses: answerFor("confirm"),
		},
	})
	require.Nil(t, reqErr)

	<-worker

	var statuses []string
	var completed map[string]any
	require.Eventually(t, func() bool {
		for {
			select {
			case n := <-subscriber.notify:
				require.Equal(t, mcp.MethodNotificationTasks, n.Method)
				var f map[string]any
				require.NoError(t, json.Unmarshal([]byte(mustMarshal(t, n.Params)), &f))
				statuses = append(statuses, f["status"].(string))
				if f["status"] == "completed" {
					completed = f
					return true
				}
			default:
				return false
			}
		}
	}, 2*time.Second, 5*time.Millisecond)

	assert.Equal(t, []string{"working", "completed"}, statuses,
		"resume and completion each push exactly once, in order")
	result, ok := completed["result"].(map[string]any)
	require.True(t, ok, "completed push inlines the result")
	content, ok := result["content"].([]any)
	require.True(t, ok)
	require.Len(t, content, 1)
	text, ok := content[0].(map[string]any)["text"].(string)
	require.True(t, ok)
	assert.Equal(t, "deployed", text)

	// The bystander heard nothing.
	select {
	case n := <-bystander.notify:
		t.Fatalf("bystander received %q for a task it never subscribed to", n.Method)
	default:
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return string(raw)
}
