package server

// Review-only interop validation for PR #987 (SEP-2663 tasks extension).
//
// Part A models the March 2026 Claude Code client exactly as observed in the
// leaked source (src/services/mcp/client.ts): initialize with
// capabilities {roots:{}, elicitation:{}}, protocol version 2025-11-25, no
// tasks support of any kind, plain tools/call with no "task" param.
//
// Part B models a modern (2026-07-28) client that does NOT declare the tasks
// extension.
//
// Part C exercises the full modern lifecycle and asserts the SEP-2663 wire
// shapes at the JSON level.

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// claudeCodeInitialize is the initialize request body the March 2026 Claude
// Code sends (client.ts:985-1001): roots + elicitation, nothing else.
const claudeCodeInitialize = `{
	"jsonrpc": "2.0", "id": 0, "method": "initialize",
	"params": {
		"protocolVersion": "2025-11-25",
		"capabilities": {"roots": {}, "elicitation": {}},
		"clientInfo": {"name": "claude-code", "title": "Claude Code", "version": "unknown"}
	}
}`

func reviewTaskServer(t *testing.T) *MCPServer {
	t.Helper()
	srv := NewMCPServer("review", "1.0.0",
		WithTaskCapabilities(true, true, true),
		WithTasksExtension(),
	)
	srv.AddTool(
		mcp.NewTool("plain_tool"),
		func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("plain sync result"), nil
		})
	srv.AddTool(
		mcp.NewTool("hybrid_tool", mcp.WithTaskSupport(mcp.TaskSupportOptional)),
		func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("hybrid sync result"), nil
		})
	srv.AddTool(
		mcp.NewTool("task_only_tool", mcp.WithTaskSupport(mcp.TaskSupportRequired)),
		func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("should not run sync"), nil
		})
	return srv
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return string(raw)
}

// TestReviewClaudeCodeMarch2026_LegacyInterop drives the server exactly like
// the March 2026 Claude Code: legacy initialize, plain tool calls.
func TestReviewClaudeCodeMarch2026_LegacyInterop(t *testing.T) {
	srv := reviewTaskServer(t)
	ctx := t.Context()

	// 1. initialize handshake succeeds and echoes the legacy version.
	resp := srv.HandleMessage(ctx, []byte(claudeCodeInitialize))
	init, ok := resp.(mcp.JSONRPCResponse)
	require.True(t, ok, "initialize: got %T", resp)
	wire := mustJSON(t, init)
	assert.Contains(t, wire, `"protocolVersion":"2025-11-25"`)

	// 2. Plain tool: ordinary CallToolResult.
	resp = srv.HandleMessage(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"plain_tool","arguments":{}}}`))
	wire = mustJSON(t, resp)
	assert.Contains(t, wire, "plain sync result")
	assert.NotContains(t, wire, `"resultType":"task"`, "must not return a task handle to a non-declaring legacy client")
	assert.NotContains(t, wire, `"task":`, "must not use the legacy task wrapper either")

	// 3. Hybrid (TaskSupportOptional) tool without the task param: still sync.
	// This is the core backward-compat guarantee for existing clients.
	resp = srv.HandleMessage(ctx, []byte(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"hybrid_tool","arguments":{}}}`))
	wire = mustJSON(t, resp)
	assert.Contains(t, wire, "hybrid sync result")
	assert.NotContains(t, wire, `"resultType":"task"`)

	// 4. Task-only tool without the task param: legacy error, unchanged from
	// main-branch behavior (Claude Code sees a clean METHOD_NOT_FOUND).
	resp = srv.HandleMessage(ctx, []byte(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"task_only_tool","arguments":{}}}`))
	errResp, ok := resp.(mcp.JSONRPCError)
	require.True(t, ok, "task-only tool: got %T", resp)
	assert.Equal(t, mcp.METHOD_NOT_FOUND, errResp.Error.Code)

	// 5. tasks/get from the legacy client on an unknown task: legacy -32602.
	resp = srv.HandleMessage(ctx, []byte(`{"jsonrpc":"2.0","id":4,"method":"tasks/get","params":{"taskId":"nope"}}`))
	errResp, ok = resp.(mcp.JSONRPCError)
	require.True(t, ok, "tasks/get unknown: got %T", resp)
	assert.Equal(t, mcp.INVALID_PARAMS, errResp.Error.Code)

	// 6. tasks/update does not exist for legacy clients.
	resp = srv.HandleMessage(ctx, []byte(`{"jsonrpc":"2.0","id":5,"method":"tasks/update","params":{"taskId":"x","inputResponses":{}}}`))
	errResp, ok = resp.(mcp.JSONRPCError)
	require.True(t, ok, "tasks/update legacy: got %T", resp)
	assert.Equal(t, mcp.METHOD_NOT_FOUND, errResp.Error.Code)
}

// modernMetaNoTasks is the _meta a modern client that does NOT declare the
// tasks extension sends on every request.
func modernMetaNoTasks() string {
	return `{"io.modelcontextprotocol/protocolVersion":"2026-07-28",
		"io.modelcontextprotocol/clientCapabilities":{"roots":{},"elicitation":{}}}`
}

func modernRequest(id int, method, extraParams string) string {
	return `{"jsonrpc":"2.0","id":` + strconv.Itoa(id) + `,"method":"` + method +
		`","params":{"_meta":` + modernMetaNoTasks() + `,` + extraParams + `}}`
}

// TestReviewModernNonDeclaringClient verifies the SEP-2663 MUSTs for a modern
// client that does not declare io.modelcontextprotocol/tasks.
func TestReviewModernNonDeclaringClient(t *testing.T) {
	srv := reviewTaskServer(t)
	ctx := WithRequestProtocolInfo(t.Context(), &RequestProtocolInfo{
		Modern:             true,
		ProtocolVersion:    "2026-07-28",
		ClientCapabilities: &mcp.ClientCapabilities{},
	})

	// tools/call on the hybrid tool: MUST NOT return CreateTaskResult.
	created, reqErr := srv.handleToolCall(ctx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "hybrid_tool"}})
	require.Nil(t, reqErr)
	_, isTask := created.(*mcp.CreateTaskResult)
	assert.False(t, isTask, "MUST NOT return CreateTaskResult to a non-declaring client")
	toolResult, ok := created.(*mcp.CallToolResult)
	require.True(t, ok, "got %T", created)
	assert.True(t, len(toolResult.Content) > 0)

	// Task-only tool: -32021 naming the extension.
	_, reqErr = srv.handleToolCall(ctx, 2, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "task_only_tool"}})
	require.NotNil(t, reqErr)
	assert.Equal(t, mcp.MISSING_REQUIRED_CLIENT_CAPABILITY, reqErr.code)

	// tasks/get | update | cancel from a non-declaring modern client: -32021.
	for i, method := range []string{"tasks/get", "tasks/update", "tasks/cancel"} {
		resp := srv.HandleMessage(ctx, []byte(modernRequest(100+i, method, `"taskId":"x","inputResponses":{}`)))
		errResp, ok := resp.(mcp.JSONRPCError)
		require.True(t, ok, "%s: got %T", resp, method)
		assert.Equal(t, mcp.MISSING_REQUIRED_CLIENT_CAPABILITY, errResp.Error.Code, method)
		wire := mustJSON(t, errResp)
		assert.Contains(t, wire, "requiredCapabilities", "%s error data names requiredCapabilities", method)
		assert.Contains(t, wire, "io.modelcontextprotocol/tasks", "%s error data names the extension", method)
	}

	// tasks/result and tasks/list are gone for modern clients.
	for i, method := range []string{"tasks/result", "tasks/list"} {
		resp := srv.HandleMessage(ctx, []byte(modernRequest(200+i, method, `"taskId":"x"`)))
		errResp, ok := resp.(mcp.JSONRPCError)
		require.True(t, ok, "%s: got %T", method, resp)
		assert.Equal(t, mcp.METHOD_NOT_FOUND, errResp.Error.Code, method)
	}
}

// TestReviewModernLifecycleWireFormat drives a full task through
// input_required to completion and asserts SEP-2663 wire shapes.
func TestReviewModernLifecycleWireFormat(t *testing.T) {
	srv := NewMCPServer("review", "1.0.0",
		WithTaskCapabilities(true, true, true),
		WithTasksExtension(),
		WithDefaultTaskTTL(300000),
	)
	const key = "confirm"
	srv.AddTool(
		mcp.NewTool("deploy", mcp.WithTaskSupport(mcp.TaskSupportOptional)),
		func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if _, ok := req.Params.InputResponses[key]; ok {
				return mcp.NewToolResultText("deployed"), nil
			}
			return NewInputRequestBuilder("step=1").
				Elicit(key, mcp.ElicitationParams{
					Mode:            mcp.ElicitationModeForm,
					Message:         "Proceed?",
					RequestedSchema: map[string]any{"type": "object"},
				}).ToolResult(), nil
		})

	ctx := WithRequestProtocolInfo(t.Context(), &RequestProtocolInfo{
		Modern:          true,
		ProtocolVersion: "2026-07-28",
		ClientCapabilities: &mcp.ClientCapabilities{
			Extensions: map[string]any{mcp.ExtensionTasks: map[string]any{}},
		},
	})

	// Create: wire shape must be resultType:"task" + inline fields + ttlMs.
	created, reqErr := srv.handleToolCall(ctx, 1, mcp.CallToolRequest{Params: mcp.CallToolParams{Name: "deploy"}})
	require.Nil(t, reqErr)
	task, ok := created.(*mcp.CreateTaskResult)
	require.True(t, ok, "got %T", created)
	require.False(t, task.Legacy)
	assert.Equal(t, mcp.ResultTypeTask, task.GetResultType())
	wire := mustJSON(t, mcp.JSONRPCResponse{
		JSONRPC: mcp.JSONRPC_VERSION,
		ID:      mcp.NewRequestId(1),
		Result:  task,
	})
	assert.Contains(t, wire, `"resultType":"task"`)
	assert.Contains(t, wire, `"ttlMs":300000`)
	assert.NotContains(t, wire, `"task":{`)
	assert.NotContains(t, wire, `"ttl":`)

	taskID := task.Task.TaskId

	// Park in input_required, then check tasks/get wire shape.
	require.Eventually(t, func() bool {
		got, e := srv.handleGetTask(ctx, 2, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
		return e == nil && got.Status == mcp.TaskStatusInputRequired
	}, 5*time.Second, 10*time.Millisecond)

	got, reqErr := srv.handleGetTask(ctx, 2, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
	require.Nil(t, reqErr)
	require.NotNil(t, got.InputRequests[key])
	wire = mustJSON(t, got)
	assert.Contains(t, wire, `"resultType":"complete"`)
	assert.Contains(t, wire, "inputRequests")

	// Partial update is ignored-then-acked; unknown keys are dropped silently.
	unknown := mcp.InputResponses{"never-issued": mcp.NewElicitationInputResponse(mcp.ElicitationResult{})}
	updated, reqErr := srv.handleUpdateTask(ctx, 3, mcp.UpdateTaskRequest{
		Params: mcp.UpdateTaskParams{TaskId: taskID, InputResponses: unknown},
	})
	require.Nil(t, reqErr, "update with only unknown keys must still ack")
	wire = mustJSON(t, updated)
	assert.True(t, strings.HasPrefix(wire, `{"resultType":"complete"`) || wire == `{"resultType":"complete"}`,
		"ack-only update result, got %s", wire)

	// Still parked: the unknown key answered nothing.
	got, reqErr = srv.handleGetTask(ctx, 4, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
	require.Nil(t, reqErr)
	assert.Equal(t, mcp.TaskStatusInputRequired, got.Status)

	// Real answer resumes the task and it completes with the result inlined.
	answers := mcp.InputResponses{key: mcp.NewElicitationInputResponse(mcp.ElicitationResult{
		ElicitationResponse: mcp.ElicitationResponse{Action: mcp.ElicitationResponseActionAccept},
	})}
	_, reqErr = srv.handleUpdateTask(ctx, 5, mcp.UpdateTaskRequest{
		Params: mcp.UpdateTaskParams{TaskId: taskID, InputResponses: answers},
	})
	require.Nil(t, reqErr)

	require.Eventually(t, func() bool {
		got, e := srv.handleGetTask(ctx, 6, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
		return e == nil && got.Status == mcp.TaskStatusCompleted
	}, 5*time.Second, 10*time.Millisecond)

	got, reqErr = srv.handleGetTask(ctx, 6, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
	require.Nil(t, reqErr)
	wire = mustJSON(t, got)
	assert.Contains(t, wire, `"status":"completed"`)
	assert.Contains(t, wire, `"result"`)
	assert.Contains(t, wire, "deployed")

	// tasks/cancel on the now-terminal task: modern path acks (no error).
	cancelled, reqErr := srv.handleCancelTask(ctx, 7, mcp.CancelTaskRequest{Params: mcp.CancelTaskParams{TaskId: taskID}})
	require.Nil(t, reqErr, "modern cancel of terminal task must ack")
	wire = mustJSON(t, cancelled)
	assert.NotContains(t, wire, "taskId", "ack-only cancel carries no task state")
	assert.Contains(t, wire, `"resultType":"complete"`)
}
