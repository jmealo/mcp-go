package server

import (
	"encoding/json"

	"github.com/mark3labs/mcp-go/mcp"
)

// detailedTaskSnapshot builds the SEP-2663 detailed view of a task: the task
// fields plus the status-specific payload (result, error, or inputRequests),
// exactly what tasks/get returns for the task's current status. Callers own
// no locks when invoking it; it takes its own read snapshot.
//
// An encode error is returned rather than silently dropped so the tasks/get
// path can surface it as an internal error.
func (s *MCPServer) detailedTaskSnapshot(entry *taskEntry) (mcp.GetTaskResult, error) {
	s.tasksMu.RLock()
	taskCopy := entry.task
	storedResult := entry.result
	resultErr := entry.resultErr
	pendingInputRequests := entry.inputRequests
	s.tasksMu.RUnlock()

	result := mcp.NewGetTaskResult(taskCopy)
	result.SetResultType(mcp.ResultTypeComplete)

	switch taskCopy.Status {
	case mcp.TaskStatusCompleted:
		if storedResult != nil {
			raw, marshalErr := json.Marshal(storedResult)
			if marshalErr != nil {
				// The result cannot be encoded. Report it as a task error
				// rather than a successful but empty payload, which would
				// silently drop the tool output.
				return result, marshalErr
			}
			result.TaskResult = json.RawMessage(raw)
		}
	case mcp.TaskStatusFailed:
		if resultErr != nil {
			result.TaskError = taskErrorDetails(resultErr)
		}
	case mcp.TaskStatusInputRequired:
		result.InputRequests = pendingInputRequests
	}

	return result, nil
}

// taskNotificationParams renders a detailed task snapshot as the params of a
// notifications/tasks push: the DetailedTask fields without the resultType
// discriminator, which belongs to results, not notifications.
func taskNotificationParams(result mcp.GetTaskResult) (mcp.NotificationParams, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return mcp.NotificationParams{}, err
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return mcp.NotificationParams{}, err
	}
	delete(fields, "resultType")
	return mcp.NotificationParams{AdditionalFields: fields}, nil
}

// notifyTaskSubscribers pushes a notifications/tasks notification carrying
// the complete DetailedTask to every session that subscribed to the task's ID
// via subscriptions/listen. Sessions without an active task subscription are
// skipped: modern task IDs are bearer handles, and an untargeted broadcast
// would leak them to sessions that never demonstrated knowledge of the ID.
//
// Delivery is best-effort: blocked notification channels drop the
// notification, and the client can always fall back to polling tasks/get.
func (s *MCPServer) notifyTaskSubscribers(entry *taskEntry) {
	result, err := s.detailedTaskSnapshot(entry)
	if err != nil {
		// The detailed payload cannot be encoded; the poll path reports the
		// same failure to the client, so there is nothing sound to push.
		return
	}
	params, err := taskNotificationParams(result)
	if err != nil {
		return
	}

	notification := mcp.JSONRPCNotification{
		JSONRPC:      mcp.JSONRPC_VERSION,
		Notification: mcp.Notification{Method: mcp.MethodNotificationTasks},
	}
	notification.Params = params

	taskID := entry.task.TaskId

	deliver := func(session any) {
		filtered, ok := session.(SessionWithSubscriptionFilter)
		if !ok {
			return
		}
		filter, active := filtered.SubscriptionFilter()
		if !active {
			return
		}
		for _, subscribed := range filter.TaskIds {
			if subscribed == taskID {
				// Error is dropped: a blocked channel must not stall task
				// execution, and the poll path remains available.
				_ = s.sendNotificationToSpecificClient(filtered, notification)
				return
			}
		}
	}

	s.sessions.Range(func(_, value any) bool {
		deliver(value)
		return true
	})
	s.listenSessions.Range(func(key, _ any) bool {
		deliver(key)
		return true
	})
}
