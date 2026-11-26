package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mark3labs/mcp-go/mcp"
)

// Streamable HTTP serves each modern subscriptions/listen with an ephemeral
// session it registers only in listenSessions (registerListenSession), which
// stores the session as the map key. notifyTaskSubscribers must reach those
// sessions too, or a modern task's subscriber over HTTP never gets a push.
func TestTaskNotifications_PushedToListenSession(t *testing.T) {
	srv := NewMCPServer("push-test", "1.0.0", WithTaskCapabilities(true, true, true))

	session := newListenSession()
	srv.registerListenSession(session)
	defer srv.unregisterListenSession(session)

	ctx, cancel := context.WithCancel(srv.WithContext(t.Context(), session))
	defer cancel()
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
				Notifications: mcp.SubscriptionFilter{TaskIds: []string{"http-task"}},
			},
		})
	}()
	defer func() { cancel(); <-done }()
	select {
	case ack := <-session.notify:
		require.Equal(t, mcp.MethodNotificationSubscriptionsAcknowledged, ack.Method)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscriptions/acknowledged")
	}

	entry, err := srv.createTask(buildModernContext(t.Context(), srv), "http-task", "deploy", nil, nil)
	require.NoError(t, err)
	srv.completeTask(entry, mcp.NewToolResultText("deployed"), nil)

	deadline := time.After(2 * time.Second)
	for {
		select {
		case n := <-session.notify:
			if n.Method == mcp.MethodNotificationTasks {
				require.Equal(t, "http-task", n.Params.AdditionalFields["taskId"])
				return
			}
		case <-deadline:
			t.Fatal("listen session got no notifications/tasks for its subscribed task")
		}
	}
}
