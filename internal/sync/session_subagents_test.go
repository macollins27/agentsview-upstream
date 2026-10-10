package sync

import (
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestTrySyncSessionWithSubagentsRejectsLaterContention(t *testing.T) {
	for _, path := range []string{"missing.jsonl", "s3://bucket/project/parent.jsonl"} {
		t.Run(path, func(t *testing.T) {
			database := openTestDB(t)
			require.NoError(t, database.UpsertSession(t.Context(), db.Session{ID: "parent", Agent: "claude", Project: "project", Machine: "local", FilePath: &path}))
			engine := NewEngine(t.Context(), database, EngineConfig{DeferStartupMaintenance: true})
			t.Cleanup(engine.Close)
			oldPaths, oldFetch, oldStat := subagentTranscriptPaths, fetchS3Object, statClaudeS3Session
			t.Cleanup(func() { subagentTranscriptPaths, fetchS3Object, statClaudeS3Session = oldPaths, oldFetch, oldStat })
			fetchS3Object = func(string) (io.ReadCloser, error) { return nil, missingS3ObjectError() }
			statClaudeS3Session = func(string) (parser.S3Object, error) { return parser.S3Object{}, missingS3ObjectError() }
			locked := make(chan struct{})
			subagentTranscriptPaths = func(string) []string {
				engine.syncMu.Lock()
				close(locked)
				return []string{path}
			}
			done := make(chan error, 1)
			go func() { done <- engine.TrySyncSessionWithSubagentsContext(t.Context(), "parent") }()
			select {
			case <-locked:
				defer engine.syncMu.Unlock()
			case <-time.After(30 * time.Second):
				t.Fatal("parent refresh did not reach descendant discovery")
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, ErrSyncInProgress)
			case <-time.After(30 * time.Second):
				t.Fatal("descendant refresh waited for another writer")
			}
		})
	}
}

func TestSyncSessionWithSubagentsDiscoversAfterMalformedUnarchivedParent(t *testing.T) {
	database := openTestDB(t)
	root := t.TempDir()
	project := filepath.Join(root, "project")
	descendants := filepath.Join(project, "parent", "subagents")
	require.NoError(t, os.MkdirAll(descendants, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(project, "parent.jsonl"), []byte("invalid json\n"), 0o600))
	content := testjsonl.ClaudeUserWithSessionIDJSON("child", "2026-05-20T10:01:00Z", "parent") + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(descendants, "agent-child.jsonl"), []byte(content), 0o600))
	engine := NewEngine(t.Context(), database, EngineConfig{AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {root}}, DeferStartupMaintenance: true})
	t.Cleanup(engine.Close)
	raw, err := sql.Open("sqlite3", database.Path())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, raw.Close()) })
	_, err = raw.ExecContext(t.Context(), `CREATE TRIGGER fail_parent BEFORE INSERT ON sessions WHEN NEW.id = 'parent' BEGIN SELECT RAISE(FAIL, 'parent refresh failed'); END`)
	require.NoError(t, err)
	require.ErrorContains(t, engine.SyncSessionWithSubagentsContext(t.Context(), "parent"), "parent refresh failed")
	parent, err := database.GetSession(t.Context(), "parent")
	require.NoError(t, err)
	require.Nil(t, parent)
	child, err := database.GetSession(t.Context(), "agent-child")
	require.NoError(t, err)
	require.NotNil(t, child)
}
