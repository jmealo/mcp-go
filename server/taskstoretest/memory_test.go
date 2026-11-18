package taskstoretest

import (
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/server"
)

func TestMemoryTaskStoreContract(t *testing.T) {
	Run(t, func(_ *testing.T, now func() time.Time) server.TaskStore {
		return server.NewMemoryTaskStore(now)
	})
}
