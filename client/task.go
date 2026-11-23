package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

// TaskInputHandlerFunc answers a task's outstanding inputRequests. It is
// invoked when ResolveTask observes status input_required; the returned
// responses are delivered with tasks/update. Returning an error aborts the
// resolution and is returned to the caller.
type TaskInputHandlerFunc func(ctx context.Context, taskID string, requests mcp.InputRequests) (mcp.InputResponses, error)

// ErrTaskNeedsInput is returned when a task parks in input_required and no
// input handler is registered to answer it.
var ErrTaskNeedsInput = errors.New("task requires input but no task input handler is registered")

// ErrInvalidResolveFallbackPollInterval is returned when ResolveTask is
// configured with a non-positive fallback polling interval.
var ErrInvalidResolveFallbackPollInterval = errors.New("fallback poll interval must be greater than zero")

// TaskFailedError reports a task that reached the failed status, carrying the
// JSON-RPC error the task inline.
type TaskFailedError struct {
	TaskID  string
	Details *mcp.JSONRPCErrorDetails
}

func (e *TaskFailedError) Error() string {
	if e.Details != nil {
		return fmt.Sprintf("task %q failed: %s", e.TaskID, e.Details.Message)
	}
	return fmt.Sprintf("task %q failed", e.TaskID)
}

// TaskCancelledError reports a task that reached the cancelled status.
type TaskCancelledError struct {
	TaskID string
}

func (e *TaskCancelledError) Error() string {
	return fmt.Sprintf("task %q was cancelled", e.TaskID)
}

// resolveOptions configures a ResolveTask call.
type resolveOptions struct {
	fallbackPollInterval time.Duration
	inputHandler         TaskInputHandlerFunc
}

// ResolveTaskOption customizes ResolveTask.
type ResolveTaskOption func(*resolveOptions)

// WithResolveFallbackPollInterval sets the polling interval used when the
// server does not suggest one. It must be positive; the server-provided
// pollIntervalMs always takes precedence.
func WithResolveFallbackPollInterval(d time.Duration) ResolveTaskOption {
	return func(o *resolveOptions) {
		o.fallbackPollInterval = d
	}
}

// WithResolveTaskInputHandler answers input_required rounds for this
// resolution, overriding the client-level handler from WithTaskInputHandler.
func WithResolveTaskInputHandler(h TaskInputHandlerFunc) ResolveTaskOption {
	return func(o *resolveOptions) {
		o.inputHandler = h
	}
}

// defaultResolveFallbackPollInterval is used when the server suggests no
// pollIntervalMs and the caller did not override the fallback.
const defaultResolveFallbackPollInterval = 500 * time.Millisecond

// ResolveTask drives a task created by the Tasks extension (SEP-2663) to a
// terminal state: it polls tasks/get at the server-suggested pollIntervalMs
// (falling back to the configured interval), answers input_required rounds
// through the registered input handler via tasks/update, and returns the
// final result reconstructed as a CallToolResult.
//
// Terminal states map to: completed -> *mcp.CallToolResult, failed ->
// *TaskFailedError, cancelled -> *TaskCancelledError. A task that parks in
// input_required with no handler registered aborts with ErrTaskNeedsInput.
func (c *Client) ResolveTask(
	ctx context.Context,
	taskID string,
	opts ...ResolveTaskOption,
) (*mcp.CallToolResult, error) {
	options := resolveOptions{
		fallbackPollInterval: defaultResolveFallbackPollInterval,
		inputHandler:         c.taskInputHandler,
	}
	for _, opt := range opts {
		opt(&options)
	}
	if options.fallbackPollInterval <= 0 {
		return nil, ErrInvalidResolveFallbackPollInterval
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		result, err := c.GetTask(ctx, mcp.GetTaskRequest{Params: mcp.GetTaskParams{TaskId: taskID}})
		if err != nil {
			return nil, fmt.Errorf("get task: %w", err)
		}

		switch {
		case result.Status == mcp.TaskStatusCompleted:
			if len(result.TaskResult) == 0 {
				return nil, fmt.Errorf("task %q completed without a result", taskID)
			}
			raw := result.TaskResult
			return mcp.ParseCallToolResult(&raw)
		case result.Status == mcp.TaskStatusFailed:
			return nil, &TaskFailedError{TaskID: taskID, Details: result.TaskError}
		case result.Status == mcp.TaskStatusCancelled:
			return nil, &TaskCancelledError{TaskID: taskID}
		case result.Status == mcp.TaskStatusInputRequired:
			if options.inputHandler == nil {
				return nil, ErrTaskNeedsInput
			}
			responses, err := options.inputHandler(ctx, taskID, result.InputRequests)
			if err != nil {
				return nil, fmt.Errorf("task input: %w", err)
			}
			if _, err := c.UpdateTask(ctx, mcp.UpdateTaskRequest{
				Params: mcp.UpdateTaskParams{
					TaskId:         taskID,
					InputResponses: responses,
				},
			}); err != nil {
				return nil, fmt.Errorf("update task: %w", err)
			}
			// The ack is eventually consistent: keep polling for the
			// resulting transition.
		}

		interval := options.fallbackPollInterval
		if result.PollInterval != nil && *result.PollInterval > 0 {
			const maxPollIntervalMilliseconds = int64(1<<63-1) / int64(time.Millisecond)
			if *result.PollInterval > maxPollIntervalMilliseconds {
				return nil, ErrServerPollIntervalOverflow
			}
			interval = time.Duration(*result.PollInterval) * time.Millisecond
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
