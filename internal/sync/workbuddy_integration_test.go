package sync_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/service"
	"go.kenn.io/agentsview/internal/sync"
)

func TestSyncSessionWithSubagentsDiscoversWorkBuddyChildrenBeforeStartupSync(t *testing.T) {
	database := dbtest.OpenTestDB(t)
	root := t.TempDir()
	engine := sync.NewEngine(t.Context(), database, sync.EngineConfig{
		AgentDirs: map[parser.AgentType][]string{parser.AgentWorkBuddy: {root}},
		Machine:   "local", DeferStartupMaintenance: true,
	})
	t.Cleanup(engine.Close)
	const parent = "11111111-1111-4111-8111-111111111111"
	parentPath := filepath.Join(root, "project", parent+".jsonl")
	write := func(path string, output int) {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		content := fmt.Sprintf(`{"type":"message","role":"assistant","content":"answer","providerData":{"model":"gpt-5","usage":{"outputTokens":%d}}}`+"\n", output)
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	}
	write(parentPath, 10)
	require.NoError(t, engine.SyncPathsContext(t.Context(), []string{parentPath}))
	childPath := filepath.Join(root, "project", parent, "subagents", "2025.01.01.jsonl")
	write(childPath, 42)
	write(filepath.Join(root, "project", parent, "subagents", "nested", "ignored.jsonl"), 100)
	write(filepath.Join(root, "project", "other.jsonl"), 100)
	const parentID = "workbuddy:" + parent
	children, err := database.GetChildSessions(t.Context(), parentID)
	require.NoError(t, err)
	require.Empty(t, children)
	require.False(t, engine.StartupReconciled())
	require.NoError(t, engine.SyncSessionWithSubagentsContext(t.Context(), parentID))
	assert.False(t, engine.StartupReconciled())
	children, err = database.GetChildSessions(t.Context(), parentID)
	require.NoError(t, err)
	require.Len(t, children, 1)
	assert.Equal(t, parentID+":subagent:2025.01.01", children[0].ID)
	usage, err := service.SessionUsageWithSubagents(t.Context(), database, parentID, true)
	require.NoError(t, err)
	require.NotNil(t, usage)
	assert.Equal(t, 52, usage.TotalOutputTokens)
	unrelated, err := database.GetSession(t.Context(), "workbuddy:other")
	require.NoError(t, err)
	assert.Nil(t, unrelated)
}
