// Package taskstoretest is a contract test suite for server.TaskStore
// implementations. Run it from the implementation's tests:
//
//	taskstoretest.Run(t, func(t *testing.T, now func() time.Time) server.TaskStore {
//		return newMyStore(t, now)
//	})
package taskstoretest

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// NewStore returns an empty store whose expiry is judged by now.
type NewStore func(t *testing.T, now func() time.Time) server.TaskStore

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func record(id, session string, status mcp.TaskStatus) server.TaskRecord {
	task := mcp.NewTask(id)
	task.Status = status
	return server.TaskRecord{
		Task:      task,
		SessionID: session,
		ToolName:  "tool",
		CreatedAt: time.Unix(1_800_000_000, 0).UTC(),
		Request:   json.RawMessage(`{"method":"tools/call","params":{"name":"tool"}}`),
	}
}

func ids(recs []server.TaskRecord) []string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.Task.TaskId)
	}
	sort.Strings(out)
	return out
}

func equalIDs(t *testing.T, what string, got []server.TaskRecord, want ...string) {
	t.Helper()
	g := ids(got)
	sort.Strings(want)
	if len(g) != len(want) {
		t.Fatalf("%s = %v, want %v", what, g, want)
	}
	for i := range g {
		if g[i] != want[i] {
			t.Fatalf("%s = %v, want %v", what, g, want)
		}
	}
}

func mustCreate(t *testing.T, s server.TaskStore, rec server.TaskRecord) server.TaskRecord {
	t.Helper()
	got, err := s.Create(context.Background(), rec)
	if err != nil {
		t.Fatalf("Create(%s): %v", rec.Task.TaskId, err)
	}
	return got
}

// Run runs every contract test against stores from newStore.
func Run(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	fresh := func(t *testing.T) (server.TaskStore, *clock) {
		c := &clock{t: time.Unix(1_800_000_000, 0)}
		return newStore(t, c.now), c
	}

	t.Run("create and get round-trip every field", func(t *testing.T) {
		s, c := fresh(t)
		exp := c.now().Add(time.Hour).UTC()
		rec := record("a", "sess", mcp.TaskStatusWorking)
		rec.Modern = true
		rec.ExpiresAt = &exp
		rec.ResumeState = json.RawMessage(`{"cursor":"x"}`)
		rec.DeliveredInputKeys = []string{"k1", "k2"}
		rec.Protocol = &server.RequestProtocolInfo{Modern: true, ProtocolVersion: "2026-07-28"}
		created := mustCreate(t, s, rec)
		if created.Revision != 1 {
			t.Fatalf("Create revision = %d, want 1", created.Revision)
		}
		got, err := s.Get(ctx, "a")
		if err != nil {
			t.Fatal(err)
		}
		want, _ := json.Marshal(created)
		have, _ := json.Marshal(got)
		if string(want) != string(have) {
			t.Fatalf("Get = %s\nwant %s", have, want)
		}
	})

	t.Run("create rejects a taken id", func(t *testing.T) {
		s, _ := fresh(t)
		mustCreate(t, s, record("a", "", mcp.TaskStatusWorking))
		if _, err := s.Create(ctx, record("a", "", mcp.TaskStatusWorking)); !errors.Is(err, server.ErrTaskExists) {
			t.Fatalf("second Create err = %v, want ErrTaskExists", err)
		}
	})

	t.Run("get, update and delete of a missing id", func(t *testing.T) {
		s, _ := fresh(t)
		if _, err := s.Get(ctx, "nope"); !errors.Is(err, server.ErrTaskNotFound) {
			t.Fatalf("Get err = %v, want ErrTaskNotFound", err)
		}
		if _, err := s.Update(ctx, record("nope", "", mcp.TaskStatusWorking)); !errors.Is(err, server.ErrTaskNotFound) {
			t.Fatalf("Update err = %v, want ErrTaskNotFound", err)
		}
		if err := s.Delete(ctx, "nope"); err != nil {
			t.Fatalf("Delete of a missing id: %v", err)
		}
	})

	t.Run("update compares the revision", func(t *testing.T) {
		s, _ := fresh(t)
		rec := mustCreate(t, s, record("a", "", mcp.TaskStatusWorking))
		rec.ResumeState = json.RawMessage(`1`)
		next, err := s.Update(ctx, rec)
		if err != nil || next.Revision != 2 {
			t.Fatalf("Update = rev %d, %v; want rev 2", next.Revision, err)
		}
		rec.ResumeState = json.RawMessage(`2`) // still revision 1: stale
		if _, err := s.Update(ctx, rec); !errors.Is(err, server.ErrTaskConflict) {
			t.Fatalf("stale Update err = %v, want ErrTaskConflict", err)
		}
		got, _ := s.Get(ctx, "a")
		if string(got.ResumeState) != "1" {
			t.Fatalf("stale Update changed the record: %s", got.ResumeState)
		}
	})

	t.Run("only one of racing updates wins", func(t *testing.T) {
		s, _ := fresh(t)
		base := mustCreate(t, s, record("a", "", mcp.TaskStatusWorking))
		const n = 16
		var wg sync.WaitGroup
		var mu sync.Mutex
		wins := 0
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := base
				r.ResumeState = json.RawMessage([]byte{'0' + byte(i%10)})
				if _, err := s.Update(ctx, r); err == nil {
					mu.Lock()
					wins++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if wins != 1 {
			t.Fatalf("%d of %d updates with the same revision succeeded, want 1", wins, n)
		}
	})

	t.Run("a terminal record never changes", func(t *testing.T) {
		for _, status := range []mcp.TaskStatus{mcp.TaskStatusCompleted, mcp.TaskStatusFailed, mcp.TaskStatusCancelled} {
			s, _ := fresh(t)
			rec := mustCreate(t, s, record("a", "", mcp.TaskStatusWorking))
			rec.Task.Status = status
			rec.Result = json.RawMessage(`{"content":[]}`)
			done, err := s.Update(ctx, rec)
			if err != nil {
				t.Fatalf("%s: terminal Update: %v", status, err)
			}
			for _, flip := range []mcp.TaskStatus{mcp.TaskStatusWorking, mcp.TaskStatusCompleted, mcp.TaskStatusCancelled} {
				r := done
				r.Task.Status = flip
				if _, err := s.Update(ctx, r); !errors.Is(err, server.ErrTaskTerminal) {
					t.Fatalf("%s -> %s: err = %v, want ErrTaskTerminal", status, flip, err)
				}
			}
			got, _ := s.Get(ctx, "a")
			if got.Task.Status != status || got.Revision != done.Revision {
				t.Fatalf("%s: record changed after terminal: %s rev %d", status, got.Task.Status, got.Revision)
			}
		}
	})

	t.Run("list by session", func(t *testing.T) {
		s, _ := fresh(t)
		mustCreate(t, s, record("s1-a", "s1", mcp.TaskStatusWorking))
		mustCreate(t, s, record("s1-b", "s1", mcp.TaskStatusCompleted))
		mustCreate(t, s, record("s2-a", "s2", mcp.TaskStatusWorking))
		mustCreate(t, s, record("shared", "", mcp.TaskStatusWorking))
		got, err := s.ListBySession(ctx, "s1")
		if err != nil {
			t.Fatal(err)
		}
		equalIDs(t, "ListBySession(s1)", got, "s1-a", "s1-b", "shared")
		got, _ = s.ListBySession(ctx, "")
		equalIDs(t, `ListBySession("")`, got, "s1-a", "s1-b", "s2-a", "shared")
		got, _ = s.ListBySession(ctx, "none")
		equalIDs(t, "ListBySession(none)", got, "shared")
	})

	t.Run("list non-terminal", func(t *testing.T) {
		s, _ := fresh(t)
		mustCreate(t, s, record("w", "", mcp.TaskStatusWorking))
		mustCreate(t, s, record("i", "", mcp.TaskStatusInputRequired))
		for _, st := range []mcp.TaskStatus{mcp.TaskStatusCompleted, mcp.TaskStatusFailed, mcp.TaskStatusCancelled} {
			mustCreate(t, s, record(string(st), "", st))
		}
		got, err := s.ListNonTerminal(ctx)
		if err != nil {
			t.Fatal(err)
		}
		equalIDs(t, "ListNonTerminal", got, "w", "i")
	})

	t.Run("expiry", func(t *testing.T) {
		s, c := fresh(t)
		exp := c.now().Add(time.Minute)
		rec := record("a", "s", mcp.TaskStatusWorking)
		rec.ExpiresAt = &exp
		rec = mustCreate(t, s, rec)
		mustCreate(t, s, record("forever", "s", mcp.TaskStatusWorking))
		c.advance(59 * time.Second)
		if _, err := s.Get(ctx, "a"); err != nil {
			t.Fatalf("Get before expiry: %v", err)
		}
		c.advance(time.Second)
		if _, err := s.Get(ctx, "a"); !errors.Is(err, server.ErrTaskExpired) {
			t.Fatalf("Get at expiry err = %v, want ErrTaskExpired", err)
		}
		if _, err := s.Update(ctx, rec); !errors.Is(err, server.ErrTaskExpired) {
			t.Fatalf("Update after expiry err = %v, want ErrTaskExpired", err)
		}
		got, _ := s.ListNonTerminal(ctx)
		equalIDs(t, "ListNonTerminal after expiry", got, "forever")
		got, _ = s.ListBySession(ctx, "s")
		equalIDs(t, "ListBySession after expiry", got, "forever")
		if err := s.Delete(ctx, "a"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "a"); !errors.Is(err, server.ErrTaskNotFound) {
			t.Fatalf("Get after Delete err = %v, want ErrTaskNotFound", err)
		}
	})

	t.Run("returned records are copies", func(t *testing.T) {
		s, _ := fresh(t)
		rec := record("a", "", mcp.TaskStatusWorking)
		rec.ResumeState = json.RawMessage(`"x"`)
		created := mustCreate(t, s, rec)
		rec.ResumeState[1] = 'y'
		created.ResumeState[1] = 'z'
		got, _ := s.Get(ctx, "a")
		if string(got.ResumeState) != `"x"` {
			t.Fatalf("store shares memory with callers: %s", got.ResumeState)
		}
	})
}
