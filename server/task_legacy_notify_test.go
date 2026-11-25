package server

// Legacy (2025-11-25) notifications/tasks/status delivery is scoped to the
// session that created the task, per that revision's wording ("to inform
// the requestor of the change") and its Security Considerations.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sidSession is a listenSession with an overridable session ID, so several
// can register on one server.
type sidSession struct {
	*listenSession
	id string
}

func (s *sidSession) SessionID() string { return s.id }

// TestLegacyTaskNotification_ScopedToCreatingSession drives a legacy task to
// completion and verifies only the creating session is informed.
func TestLegacyTaskNotification_ScopedToCreatingSession(t *testing.T) {
	srv := NewMCPServer("legacy-notify", "1.0.0", WithTaskCapabilities(true, true, true))

	creator := newListenSession()
	require.NoError(t, srv.RegisterSession(t.Context(), creator))
	bystander := &sidSession{listenSession: newListenSession(), id: "bystander"}
	require.NoError(t, srv.RegisterSession(t.Context(), bystander))

	creatorCtx := srv.WithContext(t.Context(), creator)
	entry, err := srv.createTask(creatorCtx, "legacy-task", "job", nil, nil)
	require.NoError(t, err)
	srv.completeTask(entry, mcp.NewToolResultText("done"), nil)

	select {
	case n := <-creator.notify:
		assert.Equal(t, mcp.MethodNotificationTasksStatus, n.Method)
		var fields map[string]any
		require.NoError(t, jsonUnmarshalNotification(n, &fields))
		assert.Equal(t, "legacy-task", fields["taskId"])
		assert.Equal(t, "completed", fields["status"])
	default:
		// The terminal notification may already have been consumed by the
		// status change ordering; poll briefly.
		require.Eventually(t, func() bool {
			select {
			case n := <-creator.notify:
				assert.Equal(t, mcp.MethodNotificationTasksStatus, n.Method)
				return true
			default:
				return false
			}
		}, 2*time.Second, 5*time.Millisecond)
	}

	select {
	case n := <-bystander.notify:
		t.Fatalf("bystander received %q for a task it did not create", n.Method)
	default:
	}
}

func jsonUnmarshalNotification(n mcp.JSONRPCNotification, out *map[string]any) error {
	raw, err := json.Marshal(n.Params)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}
