package sync

import (
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

type subagentParseCountingFactory struct {
	parser.ProviderFactory
	calls *atomic.Int32
}

func (f subagentParseCountingFactory) NewProvider(cfg parser.ProviderConfig) parser.Provider {
	return subagentParseCountingProvider{Provider: f.ProviderFactory.NewProvider(cfg), calls: f.calls}
}

type subagentParseCountingProvider struct {
	parser.Provider
	calls *atomic.Int32
}

func (p subagentParseCountingProvider) Parse(ctx context.Context, req parser.ParseRequest) (parser.ParseOutcome, error) {
	p.calls.Add(1)
	return p.Provider.Parse(ctx, req)
}

func TestTrySyncSessionWithSubagentsRejectsLaterContention(t *testing.T) {
	for _, tc := range []struct {
		path        string
		firstImport bool
		cancelWait  bool
	}{{"missing.jsonl", false, false}, {"s3://bucket/project/parent.jsonl", false, false}, {"new-child", true, false}, {"cancel-new-child", true, true}} {
		t.Run(tc.path, func(t *testing.T) {
			database := openTestDB(t)
			path, childPath := tc.path, "archived-child.jsonl"
			cfg := EngineConfig{DeferStartupMaintenance: true}
			var parses atomic.Int32
			if tc.firstImport {
				root := t.TempDir()
				project := filepath.Join(root, "project")
				require.NoError(t, os.MkdirAll(project, 0o755))
				path, childPath = filepath.Join(project, "parent.jsonl"), filepath.Join(project, "parent", "subagents", "agent-child.jsonl")
				require.NoError(t, os.MkdirAll(filepath.Dir(childPath), 0o755))
				require.NoError(t, os.WriteFile(path, []byte(testjsonl.ClaudeUserJSON("parent", "2026-05-20T10:00:00Z")+"\n"), 0o600))
				require.NoError(t, os.WriteFile(childPath, []byte(testjsonl.ClaudeUserWithSessionIDJSON("child", "2026-05-20T10:01:00Z", "parent")+"\n"), 0o600))
				cfg.AgentDirs = map[parser.AgentType][]string{parser.AgentClaude: {root}}
				factory, ok := parser.ProviderFactoryByType(parser.AgentClaude)
				require.True(t, ok)
				cfg.ProviderFactories = []parser.ProviderFactory{subagentParseCountingFactory{ProviderFactory: factory, calls: &parses}}
			} else {
				if strings.HasPrefix(path, "s3://") {
					childPath = "s3://bucket/project/child.jsonl"
				}
				require.NoError(t, database.UpsertSession(t.Context(), db.Session{ID: "agent-child", Agent: "claude", Project: "project", Machine: "local", FilePath: &childPath}))
			}
			require.NoError(t, database.UpsertSession(t.Context(), db.Session{ID: "parent", Agent: "claude", Project: "project", Machine: "local", FilePath: &path}))
			engine := NewEngine(t.Context(), database, cfg)
			t.Cleanup(engine.Close)
			oldPaths, oldFetch, oldStat := subagentTranscriptPaths, fetchS3Object, statClaudeS3Session
			t.Cleanup(func() { subagentTranscriptPaths, fetchS3Object, statClaudeS3Session = oldPaths, oldFetch, oldStat })
			fetchS3Object = func(string) (io.ReadCloser, error) { return nil, missingS3ObjectError() }
			statClaudeS3Session = func(string) (parser.S3Object, error) { return parser.S3Object{}, missingS3ObjectError() }
			locked := make(chan struct{})
			subagentTranscriptPaths = func(string) []string {
				engine.syncMu.Lock()
				close(locked)
				return []string{childPath}
			}
			done := make(chan error, 1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			go func() { done <- engine.TrySyncSessionWithSubagentsContext(ctx, "parent") }()
			select {
			case <-locked:
			case <-time.After(30 * time.Second):
				require.FailNow(t, "parent refresh did not reach descendant discovery")
			}
			if tc.firstImport {
				assert.Never(t, func() bool { return len(done) > 0 }, 100*time.Millisecond, 10*time.Millisecond)
				if tc.cancelWait {
					defer engine.syncMu.Unlock()
					cancel()
					select {
					case err := <-done:
						require.ErrorIs(t, err, context.Canceled)
					case <-time.After(time.Second):
						require.FailNow(t, "cancelled import still owns a writer wait")
					}
					child, err := database.GetSession(t.Context(), "agent-child")
					require.NoError(t, err)
					assert.Nil(t, child)
					return
				}
				engine.syncMu.Unlock()
				require.NoError(t, <-done)
				child, err := database.GetSession(t.Context(), "agent-child")
				require.NoError(t, err)
				require.NotNil(t, child)
				assert.EqualValues(t, 2, parses.Load(), "parent and physical child must each parse once")
				return
			}
			defer engine.syncMu.Unlock()
			select {
			case err := <-done:
				require.ErrorIs(t, err, ErrSyncInProgress)
			case <-time.After(30 * time.Second):
				require.FailNow(t, "descendant refresh waited for another writer")
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

func TestSyncSessionWithSubagentsRefreshesNativeRelationships(t *testing.T) {
	root, _, source := writeSyncGooseDB(t)
	database := openTestDB(t)
	engine := NewEngine(t.Context(), database, EngineConfig{
		AgentDirs:               map[parser.AgentType][]string{parser.AgentGoose: {root}},
		DeferStartupMaintenance: true,
	})
	t.Cleanup(engine.Close)
	for _, tc := range []struct{ id, parent string }{{"child", "session-001"}, {"fork", "child"}} {
		_, err := source.ExecContext(t.Context(), `INSERT INTO sessions (id, working_dir, parent_session_id, session_type) VALUES (?, '/work/project', ?, 'sub_agent')`, tc.id, tc.parent)
		require.NoError(t, err)
	}
	for _, id := range []string{"session-001", "child", "fork"} {
		require.NoError(t, engine.SyncSingleSessionContext(t.Context(), "goose:"+id))
	}
	fork, err := database.GetSession(t.Context(), "goose:fork")
	require.NoError(t, err)
	require.NotNil(t, fork)
	fork.RelationshipType = "fork"
	require.NoError(t, database.UpsertSession(t.Context(), *fork))
	for _, id := range []string{"child", "fork"} {
		_, err := source.ExecContext(t.Context(), `INSERT INTO messages (message_id, session_id, role, content_json, created_timestamp) VALUES ('fresh', ?, 'user', '[{"type":"text","text":"fresh descendant"}]', 1700000020)`, id)
		require.NoError(t, err)
	}
	require.NoError(t, engine.TrySyncSessionWithSubagentsContext(t.Context(), "goose:session-001"))
	for _, id := range []string{"child", "fork"} {
		messages, err := database.GetMessages(t.Context(), "goose:"+id, 0, 100, true)
		require.NoError(t, err)
		require.Len(t, messages, 1, "native child and its fork must refresh")
		assert.Equal(t, "fresh descendant", messages[0].Content)
	}
}
