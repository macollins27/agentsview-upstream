package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestDaemonArchiveQueryBackendDefaultsMappedLocalTimezone(t *testing.T) {
	t.Setenv("TZ", "America/New_York")
	oldLocal := time.Local                                         //nolint:forbidigo // Exercise report formatting and date buckets in the local calendar timezone.
	time.Local = time.FixedZone("Eastern Standard Time", -5*60*60) //nolint:forbidigo // Exercise report formatting and date buckets in the local calendar timezone.
	t.Cleanup(func() { time.Local = oldLocal })                    //nolint:forbidigo // Exercise report formatting and date buckets in the local calendar timezone.

	var queries []url.Values
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query())
		writeJSONResponse(w, `{"timezone":"America/New_York"}`)
	}))
	t.Cleanup(ts.Close)
	backend := daemonArchiveQueryBackend{tr: transport{URL: ts.URL}}

	_, err := backend.ActivityReport(t.Context(), ActivityReportConfig{
		Preset: "day",
	})
	require.NoError(t, err)
	require.Len(t, queries, 1)
	assert.Equal(t, "America/New_York", queries[0].Get("timezone"))
	assert.Equal(t, todayIn("America/New_York"), queries[0].Get("date"))

	_, err = backend.ActivityReport(t.Context(), ActivityReportConfig{
		Preset: "day", Date: "2026-03-09", Timezone: "Europe/Berlin",
	})
	require.NoError(t, err)
	require.Len(t, queries, 2)
	assert.Equal(t, "Europe/Berlin", queries[1].Get("timezone"))
	assert.Equal(t, "2026-03-09", queries[1].Get("date"))
}

func TestResolveArchiveQueryBackendNoSyncStartsNoSyncDaemon(t *testing.T) {
	testDataDir(t)
	var started bool
	stubStartBackgroundServeForTransport(t, func(
		_ context.Context, cfg *config.Config, _ time.Duration, _ bool,
	) (*DaemonRuntime, error) {
		started = true
		assert.True(t, cfg.NoSync)
		return &DaemonRuntime{Host: "127.0.0.1", Port: 12345}, nil
	})

	backend := resolveTestArchiveQueryBackend(t, defaultArchiveQueryPolicy(
		func(p *archiveQueryPolicy) { p.NoSync = true },
	))
	assert.True(t, started)
	assert.IsType(t, daemonArchiveQueryBackend{}, backend)
}

func TestResolveArchiveQueryBackendRefusesReadOnlyDaemonForFreshQueries(t *testing.T) {
	dataDir := testDataDir(t)

	var called bool
	ts := sessionUsageRuntimeServer(t, func(
		w http.ResponseWriter, r *http.Request,
	) {
		called = true
		w.WriteHeader(http.StatusInternalServerError)
	})
	registerTestRuntime(t, dataDir, ts.URL, true)

	_, cleanup, err := resolveArchiveQueryBackend(
		t.Context(), defaultArchiveQueryPolicy(nil),
	)
	if cleanup != nil {
		t.Cleanup(cleanup)
	}
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")
	assert.NotContains(t, err.Error(), "--pg")
	assert.False(t, called)
}

func TestResolveArchiveQueryBackendUsesGeneratedAutostartToken(t *testing.T) {
	testDataDir(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer generated-token", r.Header.Get("Authorization"))
		assert.Equal(t, "/api/v1/sync", r.URL.Path)
		assert.Equal(t, "true", r.URL.Query().Get("startup_only"))
		writeJSONResponse(w, `{}`)
	}))
	t.Cleanup(ts.Close)

	stubStartBackgroundServeForTransport(t, func(
		_ context.Context, cfg *config.Config, _ time.Duration, _ bool,
	) (*DaemonRuntime, error) {
		cfg.AuthToken = "generated-token"
		return daemonRuntimeFromTestURL(t, ts.URL), nil
	})

	backend := resolveTestArchiveQueryBackend(t, defaultArchiveQueryPolicy(
		func(p *archiveQueryPolicy) {
			p.AutoStart = true
			p.ReadOnlyDaemon = archiveQueryRejectReadOnlyDaemon
		},
	))

	daemonBackend, ok := backend.(daemonArchiveQueryBackend)
	require.True(t, ok)
	assert.Equal(t, "generated-token", daemonBackend.authToken)
}

func TestLocalArchiveQuerySessionUsageNoSyncSkipsSingleSessionSync(
	t *testing.T,
) {
	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, "sessions.db")
	writer := dbtest.OpenTestDBAt(t, dbPath)
	started := "2026-06-23T12:00:00Z"
	require.NoError(t, writer.UpsertSession(t.Context(), db.Session{
		ID:                   "codex:no-sync-usage",
		Project:              "proj",
		Machine:              "local",
		Agent:                "codex",
		StartedAt:            &started,
		MessageCount:         1,
		TotalOutputTokens:    42,
		HasTotalOutputTokens: true,
	}))
	require.NoError(t, writer.Close())

	readonly, err := db.OpenReadOnly(t.Context(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { readonly.Close() })

	backend := localArchiveQueryBackend{
		cfg:           config.Config{DBPath: dbPath},
		database:      readonly,
		offline:       true,
		skipFreshData: true,
	}
	stderr := captureStderr(t, func() {
		out, exitCode, err := backend.SessionUsage(
			t.Context(),
			sessionUsageQuery{SessionID: "codex:no-sync-usage"},
		)
		require.NoError(t, err)
		require.NotNil(t, out)
		assert.Equal(t, tokenUseExitOK, exitCode)
		assert.Equal(t, 42, out.TotalOutputTokens)
	})
	assert.NotContains(t, stderr, "warning: sync failed")
	assert.NotContains(t, stderr, "warning: pricing seed failed")
}

// TestLocalSessionUsageRefreshesSubagentTranscripts covers the freshness
// half of the subagent rollup: SyncSingleSession only knows about the named
// session's file, so the backend must also ingest the agent-*.jsonl files
// beside it. Without that, a session that just finished would report a
// combined cost missing its most recent subagents.
func TestLocalSessionUsageRefreshesSubagentTranscripts(t *testing.T) {
	dataDir := testDataDir(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, def := range parser.Registry {
		if def.EnvVar != "" {
			t.Setenv(def.EnvVar,
				filepath.Join(home, "agent-dirs", string(def.Type)))
		}
	}
	claudeDir := t.TempDir()
	t.Setenv("CLAUDE_PROJECTS_DIR", claudeDir)

	projDir := filepath.Join(claudeDir, "-home-proj")
	require.NoError(t, os.MkdirAll(
		filepath.Join(projDir, "parent-uuid", "subagents"), 0o755))
	parentPath := filepath.Join(projDir, "parent-uuid.jsonl")
	require.NoError(t, os.WriteFile(parentPath, []byte(
		testjsonl.NewSessionBuilder().
			AddClaudeUser("2026-05-20T10:00:00Z", "delegate this").
			AddClaudeAssistant("2026-05-20T10:00:05Z", "on it").
			String(),
	), 0o644))

	dbPath := sessionsDBPath(dataDir)
	database := dbtest.OpenTestDBAt(t, dbPath)
	backend := localArchiveQueryBackend{
		cfg: config.Config{
			DBPath: dbPath,
			AgentDirs: map[parser.AgentType][]string{
				parser.AgentClaude: {claudeDir},
			},
		},
		database: database,
		offline:  true,
	}
	ctx := t.Context()

	// The archived child must refresh before the combined usage query.
	_, _, err := backend.SessionUsage(
		ctx, sessionUsageQuery{SessionID: "parent-uuid"})
	require.NoError(t, err)

	childPath := filepath.Join(projDir, "parent-uuid", "subagents", "agent-worker1.jsonl")
	parentID := "parent-uuid"
	require.NoError(t, database.UpsertSession(ctx, db.Session{
		ID: "agent-worker1", Agent: "claude", Machine: "local", Project: "project",
		FilePath: &childPath, ParentSessionID: &parentID, RelationshipType: "subagent",
	}))
	require.NoError(t, os.WriteFile(
		childPath,
		[]byte(testjsonl.NewSessionBuilder().
			AddClaudeUserWithSessionID(
				"2026-05-20T10:01:00Z", "do the subtask", "parent-uuid").
			AddClaudeAssistantUsage("2026-05-20T10:01:30Z", "subtask done", testjsonl.ClaudeAssistantUsage{OutputTokens: 42}).
			String()),
		0o644))

	// Passive reads must keep the archived child usage.
	archived, _, err := backend.SessionUsage(
		ctx, sessionUsageQuery{SessionID: "parent-uuid", NoSync: true})
	require.NoError(t, err)
	require.NotNil(t, archived)
	assert.Equal(t, 1, archived.SubagentCount)
	assert.Zero(t, archived.TotalOutputTokens)
	unsynced, err := database.GetSession(ctx, "agent-worker1")
	require.NoError(t, err)
	require.NotNil(t, unsynced)
	assert.Zero(t, unsynced.TotalOutputTokens)

	out, _, err := backend.SessionUsage(
		ctx, sessionUsageQuery{SessionID: "parent-uuid"})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, 42, out.TotalOutputTokens)
	assert.Equal(t, 1, out.SubagentCount,
		"the archived subagent must refresh before the query")

	child, err := database.GetSession(ctx, "agent-worker1")
	require.NoError(t, err)
	require.NotNil(t, child, "subagent session was not synced")
	require.NotNil(t, child.ParentSessionID)
	assert.Equal(t, "parent-uuid", *child.ParentSessionID)

	archived, _, err = backend.SessionUsage(
		ctx, sessionUsageQuery{SessionID: "parent-uuid", NoSync: true})
	require.NoError(t, err)
	require.NotNil(t, archived)
	assert.Equal(t, out.SessionUsage, archived.SessionUsage)

	// --own-only skips the subagent refresh and the combined view.
	own, _, err := backend.SessionUsage(
		ctx, sessionUsageQuery{SessionID: "parent-uuid", OwnOnly: true})
	require.NoError(t, err)
	require.NotNil(t, own)
	assert.Zero(t, own.SubagentCount)
}

func TestLocalSessionUsageStaleArchiveKeepsStoredUsage(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	sourcePath := filepath.Join(cfg.AgentDirs[parser.AgentClaude][0], "-home-proj1", "session1.jsonl")
	require.NoError(t, os.WriteFile(sourcePath, []byte(testjsonl.NewSessionBuilder().
		AddClaudeUser("2026-05-20T10:00:00Z", "original").
		AddClaudeAssistantUsage("2026-05-20T10:00:05Z", "original usage", testjsonl.ClaudeAssistantUsage{OutputTokens: 37}).String()), 0o644))
	database, err := db.Open(t.Context(), cfg.DBPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	backend := localArchiveQueryBackend{cfg: cfg, database: database, offline: true}
	before, _, err := backend.SessionUsage(t.Context(), sessionUsageQuery{SessionID: "session1"})
	require.NoError(t, err)
	require.NotNil(t, before)
	require.NoError(t, os.WriteFile(sourcePath, []byte(testjsonl.NewSessionBuilder().
		AddClaudeUser("2026-05-20T10:00:00Z", "changed").
		AddClaudeAssistantUsage("2026-05-20T10:00:05Z", "changed usage", testjsonl.ClaudeAssistantUsage{OutputTokens: 999}).String()), 0o644))
	require.NoError(t, database.Close())
	markArchiveStale(t, cfg.DBPath)
	database, err = db.Open(t.Context(), cfg.DBPath)
	require.NoError(t, err)
	require.True(t, database.NeedsResync())
	backend.database = database
	for _, ownOnly := range []bool{false, true} {
		stderr := captureStderr(t, func() {
			after, code, err := backend.SessionUsage(t.Context(), sessionUsageQuery{SessionID: before.SessionID, OwnOnly: ownOnly})
			require.NoError(t, err)
			require.NotNil(t, after)
			assert.Equal(t, 0, code)
			assert.Equal(t, 37, after.TotalOutputTokens)
			assert.True(t, after.HasTokenData)
		})
		assert.Contains(t, stderr, "run agentsview resync")
		assert.Contains(t, stderr, "showing stored usage")
		assert.True(t, database.NeedsResync())
	}
}
