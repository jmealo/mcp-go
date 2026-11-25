package server

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

// taskStreamListener is one subscriptions/listen stream that opted into at
// least one task's notifications. finished has one channel per acknowledged
// taskId; a channel closes when that task's terminal notification has been
// delivered (or the task expired without one), and done closes when every
// acknowledged task has finished - the stream's natural end.
type taskStreamListener struct {
	session ClientSession

	mu       sync.Mutex
	finished map[string]chan struct{}
}

func (l *taskStreamListener) finish(taskID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ch, ok := l.finished[taskID]; ok {
		delete(l.finished, taskID)
		close(ch)
	}
}

// done reports whether every acknowledged task has finished.
func (l *taskStreamListener) done() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.finished) == 0
}

// waitChans returns the open finish channels, snapshot under the lock.
func (l *taskStreamListener) waitChans() []<-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	chans := make([]<-chan struct{}, 0, len(l.finished))
	for _, ch := range l.finished {
		chans = append(chans, ch)
	}
	return chans
}

// taskListenerRegistry indexes task-notification listeners by task ID, so
// fan-out touches only the sessions subscribed to the transitioning task
// instead of walking every session for every transition.
type taskListenerRegistry struct {
	mu     sync.Mutex
	byTask map[string]map[*taskStreamListener]struct{}
}

func (r *taskListenerRegistry) register(taskIDs []string, listener *taskStreamListener) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, taskID := range taskIDs {
		set, ok := r.byTask[taskID]
		if !ok {
			set = make(map[*taskStreamListener]struct{})
			r.byTask[taskID] = set
		}
		set[listener] = struct{}{}
	}
}

func (r *taskListenerRegistry) unregister(listener *taskStreamListener) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for taskID, set := range r.byTask {
		delete(set, listener)
		if len(set) == 0 {
			delete(r.byTask, taskID)
		}
	}
}

// listeners returns the streams subscribed to taskID.
func (r *taskListenerRegistry) listeners(taskID string) []*taskStreamListener {
	r.mu.Lock()
	defer r.mu.Unlock()
	set := r.byTask[taskID]
	out := make([]*taskStreamListener, 0, len(set))
	for l := range set {
		out = append(out, l)
	}
	return out
}

// resolveTaskSubscriptions narrows requested task IDs to those the caller
// may subscribe to: the task must exist (or be loadable from the store) and
// pass the task authorizer, which also applies the era and session
// isolation rules. Unknown, expired, and unauthorized IDs are silently
// dropped - the acknowledged filter may be narrower than the request.
func (s *MCPServer) resolveTaskSubscriptions(ctx context.Context, taskIDs []string) []string {
	var kept []string
	for _, taskID := range taskIDs {
		if _, err := s.getTaskEntry(ctx, taskID, TaskOpSubscribe); err != nil {
			continue
		}
		kept = append(kept, taskID)
	}
	return kept
}

// taskStreamKey identifies the caller whose concurrent task listen streams
// are counted against WithMaxTaskListenStreams: the owner identity when
// WithTaskOwner is configured, else the session.
func (s *MCPServer) taskStreamKey(ctx context.Context, session ClientSession) string {
	if s.taskOwnerIDFn != nil {
		if owner := s.taskOwnerIDFn(ctx); owner != "" {
			return "owner:" + owner
		}
	}
	return "session:" + session.SessionID()
}

// acquireTaskListenStream enforces the per-caller concurrent stream cap.
// It returns a release function; a zero limit disables the cap.
func (s *MCPServer) acquireTaskListenStream(key string) (func(), error) {
	s.taskListenMu.Lock()
	defer s.taskListenMu.Unlock()
	if s.maxTaskListenStreams > 0 && s.taskListenStreams[key] >= s.maxTaskListenStreams {
		return nil, errTooManyTaskListenStreams
	}
	s.taskListenStreams[key]++
	var once sync.Once
	return func() {
		once.Do(func() {
			s.taskListenMu.Lock()
			defer s.taskListenMu.Unlock()
			if s.taskListenStreams[key] > 0 {
				s.taskListenStreams[key]--
				if s.taskListenStreams[key] == 0 {
					delete(s.taskListenStreams, key)
				}
			}
		})
	}, nil
}

// watchTaskStream ends the stream when every acknowledged task has reached a
// terminal state and had its final notification delivered, or has expired.
// Locally-run tasks are finished by the notification path; tasks this
// process does not run (store-only, taken over, or removed by TTL) are
// detected by polling, which also covers expiry.
func (s *MCPServer) watchTaskStream(ctx context.Context, listener *taskStreamListener) {
	interval := s.remoteTaskPoll
	if interval <= 0 {
		// Without a store the interval only paces expiry detection; local
		// tasks finish event-driven via the notification path.
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if listener.done() {
			return
		}
		for _, taskID := range s.pendingTaskIDs(listener) {
			finished := s.taskFinishedOutsideNotify(taskID, ctx)
			if finished {
				listener.finish(taskID)
			}
		}
		if listener.done() {
			return
		}
		storeDone := (<-chan struct{})(nil)
		if s.taskStoreCtx != nil {
			storeDone = s.taskStoreCtx.Done()
		}
		// Wake as soon as any acknowledged task finishes (the notification
		// path closes its channel), not only on the next poll tick.
		cases := make([]reflect.SelectCase, 0, len(listener.waitChans())+3)
		cases = append(cases,
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(storeDone)},
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ticker.C)},
		)
		for _, ch := range listener.waitChans() {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
		}
		chosen, _, _ := reflect.Select(cases)
		switch chosen {
		case 0, 1:
			return
		}
	}
}

// pendingTaskIDs snapshots the listener's unfinished task IDs.
func (s *MCPServer) pendingTaskIDs(listener *taskStreamListener) []string {
	listener.mu.Lock()
	defer listener.mu.Unlock()
	ids := make([]string, 0, len(listener.finished))
	for taskID := range listener.finished {
		ids = append(ids, taskID)
	}
	return ids
}

// taskFinishedOutsideNotify reports whether a task needs no further
// notification on this stream: the record is gone (expired or deleted), or
// a store record is terminal while this process never ran the task.
func (s *MCPServer) taskFinishedOutsideNotify(taskID string, ctx context.Context) bool {
	s.tasksMu.RLock()
	entry, local := s.tasks[taskID]
	tombstone := false
	if !local {
		_, tombstone = s.expiredTasks[taskID]
	}
	s.tasksMu.RUnlock()
	if local {
		// The notify path owns locally-run tasks, including their terminal
		// delivery; nothing to do here.
		_ = entry
		return false
	}
	if tombstone {
		return true
	}
	if s.taskStore == nil {
		// No store and no entry: the task ended and was reaped, or never
		// existed on this server; nothing further can arrive for it.
		return true
	}
	rec, err := s.taskStore.Get(ctx, taskID)
	if err != nil {
		// Unknown or expired in the store, or the store is failing: treat
		// unresolvable IDs as finished so the stream cannot hang on them.
		return true
	}
	return rec.Task.Status.IsTerminal() && rec.Owner == s.taskOwner
}

// notifyTaskListeners delivers a task notification to the registered
// listeners and, for a terminal status, marks the task finished on each
// stream after the delivery attempt.
func (s *MCPServer) notifyTaskListeners(taskID string, notification mcp.JSONRPCNotification, terminal bool) {
	for _, listener := range s.taskListeners.listeners(taskID) {
		// A blocked channel must not stall task execution; the poll path
		// remains available to the client.
		_ = s.sendNotificationToSpecificClient(listener.session, notification)
		if terminal {
			listener.finish(taskID)
		}
	}
}
