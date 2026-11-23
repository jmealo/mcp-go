package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTaskServer builds an in-process server with one task-augmented tool
// whose handler is supplied by the test.
func newTaskServer(t *testing.T, handler server.ToolHandlerFunc) *server.MCPServer {
	t.Helper()
	srv := server.NewMCPServer("task-test", "1.0.0",
		server.WithTaskCapabilities(true, true, true),
	)
	srv.AddTool(
		mcp.NewTool("deploy", mcp.WithTaskSupport(mcp.TaskSupportRequired)),
		handler,
	)
	return srv
}

func startClient(t *testing.T, srv *server.MCPServer, opts ...ClientOption) *Client {
	t.Helper()
	c, err := NewInProcessClient(srv)
	require.NoError(t, err)
	for _, opt := range append([]ClientOption{
		WithProtocolVersion(mcp.ProtocolVersion20260728),
	}, opts...) {
		opt(c)
	}
	require.NoError(t, c.Start(t.Context()))
	t.Cleanup(func() { _ = c.Close() })
	_, err = c.Initialize(t.Context(), mcp.InitializeRequest{})
	require.NoError(t, err)
	return c
}

// TestCallTaskTool_WithoutResolutionSurfacesTypedError verifies the opt-in
// contract: with the extension declared but resolution disabled, a task
// handle reaches the caller as UnsupportedTaskResultError.
func TestCallTaskTool_WithoutResolutionSurfacesTypedError(t *testing.T) {
	srv := newTaskServer(t, func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("done"), nil
	})
	c := startClient(t, srv, WithClientCapabilities(mcp.ClientCapabilities{
		Extensions: map[string]any{mcp.ExtensionTasks: map[string]any{}},
	}))

	_, err := c.CallTool(t.Context(), mcp.CallToolRequest{
		Params: mcp.CallToolParams{Name: "deploy"},
	})
	var handle *mcp.UnsupportedTaskResultError
	require.ErrorAs(t, err, &handle)
	assert.NotEmpty(t, handle.TaskID)
}

// TestCallTaskTool_TransparentResolution drives a task to completion inside
// CallTool: the caller sees only the final result.
func TestCallTaskTool_TransparentResolution(t *testing.T) {
	srv := newTaskServer(t, func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		time.Sleep(20 * time.Millisecond)
		return mcp.NewToolResultText("deployed"), nil
	})
	c := startClient(t, srv, WithTaskResolution())

	result, err := c.CallTool(t.Context(), mcp.CallToolRequest{
		Params: mcp.CallToolParams{Name: "deploy"},
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)
	assert.Equal(t, "deployed", result.Content[0].(mcp.TextContent).Text)
}

// TestCallTaskTool_ResolutionWithInput answers an input_required round
// through the registered handler.
func TestCallTaskTool_ResolutionWithInput(t *testing.T) {
	const key = "confirm"
	srv := newTaskServer(t, func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if _, answered := req.Params.InputResponses[key]; answered {
			return mcp.NewToolResultText("confirmed and deployed"), nil
		}
		return server.NewInputRequestBuilder("round-1").
			Elicit(key, mcp.ElicitationParams{
				Mode:            mcp.ElicitationModeForm,
				Message:         "Proceed?",
				RequestedSchema: map[string]any{"type": "object"},
			}).ToolResult(), nil
	})

	var seenTaskID string
	c := startClient(t, srv,
		WithTaskResolution(),
		WithTaskInputHandler(func(_ context.Context, taskID string, requests mcp.InputRequests) (mcp.InputResponses, error) {
			seenTaskID = taskID
			require.Contains(t, requests, key)
			return mcp.InputResponses{key: mcp.NewElicitationInputResponse(mcp.ElicitationResult{
				ElicitationResponse: mcp.ElicitationResponse{Action: mcp.ElicitationResponseActionAccept},
			})}, nil
		}),
	)

	result, err := c.CallTool(t.Context(), mcp.CallToolRequest{
		Params: mcp.CallToolParams{Name: "deploy"},
	})
	require.NoError(t, err)
	assert.Equal(t, "confirmed and deployed", result.Content[0].(mcp.TextContent).Text)
	assert.NotEmpty(t, seenTaskID)
}

// TestCallTaskTool_ResolutionFailedTask maps the failed status to the typed
// error carrying the task's JSON-RPC error details.
func TestCallTaskTool_ResolutionFailedTask(t *testing.T) {
	srv := newTaskServer(t, func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return nil, errors.New("deployment exploded")
	})
	c := startClient(t, srv, WithTaskResolution())

	_, err := c.CallTool(t.Context(), mcp.CallToolRequest{
		Params: mcp.CallToolParams{Name: "deploy"},
	})
	var failed *TaskFailedError
	require.ErrorAs(t, err, &failed)
	assert.Contains(t, failed.Error(), "deployment exploded")
}

// TestCallTaskTool_ResolutionCancelledTask cancels a running task and maps
// the cancelled status to the typed error.
func TestCallTaskTool_ResolutionCancelledTask(t *testing.T) {
	block := make(chan struct{})
	taskCreated := make(chan string, 1)
	hooks := &server.TaskHooks{}
	hooks.AddOnTaskCreated(func(_ context.Context, metrics server.TaskMetrics) {
		select {
		case taskCreated <- metrics.TaskID:
		default:
		}
	})

	srv := server.NewMCPServer("task-test", "1.0.0",
		server.WithTaskCapabilities(true, true, true),
		server.WithTaskHooks(hooks),
	)
	srv.AddTool(
		mcp.NewTool("deploy", mcp.WithTaskSupport(mcp.TaskSupportRequired)),
		func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			select {
			case <-block:
				return mcp.NewToolResultText("late"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	)
	defer close(block)

	c := startClient(t, srv, WithTaskResolution())

	done := make(chan error, 1)
	go func() {
		_, err := c.CallTool(t.Context(), mcp.CallToolRequest{
			Params: mcp.CallToolParams{Name: "deploy"},
		})
		done <- err
	}()

	var taskID string
	select {
	case taskID = <-taskCreated:
	case <-time.After(2 * time.Second):
		t.Fatal("task was never created")
	}

	_, err := c.CancelTask(t.Context(), mcp.CancelTaskRequest{
		Params: mcp.CancelTaskParams{TaskId: taskID},
	})
	require.NoError(t, err)

	select {
	case err := <-done:
		var cancelled *TaskCancelledError
		require.ErrorAs(t, err, &cancelled)
		assert.Equal(t, taskID, cancelled.TaskID)
	case <-time.After(5 * time.Second):
		t.Fatal("CallTool did not return after cancellation")
	}
}
