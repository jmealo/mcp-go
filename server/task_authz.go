package server

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// TaskOp names the operation a task authorization decision is made for.
type TaskOp string

const (
	// TaskOpGet authorizes a tasks/get request.
	TaskOpGet TaskOp = "get"
	// TaskOpResult authorizes a legacy tasks/result request.
	TaskOpResult TaskOp = "result"
	// TaskOpUpdate authorizes a tasks/update request.
	TaskOpUpdate TaskOp = "update"
	// TaskOpCancel authorizes a tasks/cancel request.
	TaskOpCancel TaskOp = "cancel"
	// TaskOpSubscribe authorizes subscribing to a task's status
	// notifications via subscriptions/listen.
	TaskOpSubscribe TaskOp = "subscribe"
)

// TaskAuthorizerFunc decides whether the caller behind ctx may perform op on
// the task whose stored record is rec. It runs after the server has decoded
// the request and loaded the record, so it judges the same values the server
// acts on - a request whose decoding the application's own stricter parser
// would reject cannot slip past it.
//
// Any error denies the request, and a denial is indistinguishable on the wire
// from the task not existing: both answer -32602 "task not found", so a
// caller cannot probe for other callers' task IDs.
type TaskAuthorizerFunc func(ctx context.Context, op TaskOp, rec TaskRecord) error

// TaskOwnerFunc returns the opaque identity of the caller behind ctx. It is
// recorded on every task the server creates (TaskRecord.OwnerID) and
// persisted through the TaskStore, so authorizers can compare the record's
// creator against the requester across restarts and server instances.
type TaskOwnerFunc func(ctx context.Context) string

// WithTaskAuthorizer sets the authorization hook consulted for tasks/get,
// tasks/result, tasks/update, tasks/cancel, and every taskId a client
// subscribes to via subscriptions/listen. Without it, task access keeps its
// existing session and cross-era isolation.
func WithTaskAuthorizer(fn TaskAuthorizerFunc) ServerOption {
	return func(s *MCPServer) {
		s.taskAuthorizer = fn
	}
}

// WithTaskOwner records the caller's identity on created tasks, retrievable
// by a TaskAuthorizerFunc as TaskRecord.OwnerID. The identity is opaque to
// the SDK.
func WithTaskOwner(fn TaskOwnerFunc) ServerOption {
	return func(s *MCPServer) {
		s.taskOwnerIDFn = fn
	}
}

// authorizeTask applies the configured TaskAuthorizerFunc to entry's current
// state. A nil authorizer authorizes everything. Every authorizer error,
// denial or otherwise, maps to the same error the not-found path returns:
// the existence of a task must not leak through authorization failures.
func (s *MCPServer) authorizeTask(ctx context.Context, op TaskOp, entry *taskEntry) error {
	if s.taskAuthorizer == nil {
		return nil
	}
	s.tasksMu.RLock()
	rec := s.recordFromEntryLocked(entry)
	s.tasksMu.RUnlock()
	if err := s.taskAuthorizer(ctx, op, rec); err != nil {
		return fmt.Errorf("task not found")
	}
	return nil
}

// errTooManyTaskListenStreams refuses a stream over the per-caller cap.
var errTooManyTaskListenStreams = errors.New("task listen stream limit reached for this caller")

// WithMaxTaskListenStreams caps the concurrent subscriptions/listen streams
// that carry task notifications, counted per caller: the WithTaskOwner
// identity when configured, otherwise the session. A caller over the cap
// has its stream refused with INVALID_PARAMS. Zero (the default) disables
// the cap.
func WithMaxTaskListenStreams(n int) ServerOption {
	return func(s *MCPServer) {
		if n > 0 {
			s.maxTaskListenStreams = n
		}
	}
}

// WithTaskListenMaxLifetime bounds how long a task-notification listen
// stream may run; the stream then closes and the client may re-subscribe.
// Zero (the default) disables the bound.
func WithTaskListenMaxLifetime(d time.Duration) ServerOption {
	return func(s *MCPServer) {
		if d > 0 {
			s.taskListenMaxLifetime = d
		}
	}
}
