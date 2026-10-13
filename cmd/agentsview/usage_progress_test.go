package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/server"
	"go.kenn.io/agentsview/internal/service"
	agentsync "go.kenn.io/agentsview/internal/sync"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestUsageCommandsSkipStartupExceptStatusline(t *testing.T) {
	for _, command := range []string{"daily", "statusline", "session usage", "token-use"} {
		t.Run(command, func(t *testing.T) {
			newAgentDataDir(t)
			var ts *httptest.Server
			if command == "daily" || command == "statusline" {
				ts = sessionUsageRuntimeServer(t, func(w http.ResponseWriter, r *http.Request) {
					if command == "daily" {
						writeUsageStreamResponse(t, w, r, sampleDailyUsageJSON)
					} else {
						writeJSONResponse(w, sampleDailyUsageJSON)
					}
				})
			} else {
				ts, _ = newRemoteUsageServer(t, remoteUsageSpec{canonicalID: "codex:session-a"})
			}
			started := false
			stubStartBackgroundServeForTransport(t, func(ctx context.Context, cfg *config.Config, _ time.Duration, _ bool) (*DaemonRuntime, error) {
				started = true
				if command == "statusline" {
					deadline, ok := ctx.Deadline()
					assert.True(t, ok, "prompt refresh must have a deadline")
					if ok {
						assert.InDelta(t, 30, time.Until(deadline).Seconds(), 1)
					}
				}
				assert.Equal(t, command != "statusline", cfg.SkipInitialSync)
				assert.False(t, cfg.NoSync)
				return daemonRuntimeFromTestURL(t, ts.URL), nil
			})
			captureStdout(t, func() {
				switch command {
				case "daily":
					runUsageDaily(UsageDailyConfig{JSON: true, Timezone: "UTC"})
				case "statusline":
					runUsageStatusline(UsageStatuslineConfig{JSON: true})
				case "session usage":
					cmd := sessionUsageCommand(t, "session", "usage", "codex:session-a")
					_, _, err := sessionUsageDataForCommand(cmd, "codex:session-a")
					require.NoError(t, err)
				case "token-use":
					_, _, err := sessionUsageData(t.Context(), "codex:session-a")
					require.NoError(t, err)
				}
			})
			assert.True(t, started, "exercise daemon startup rather than an existing daemon")
		})
	}
}

func TestUsageCommandsReconcileDaemonStartedByDaily(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	database := dbtest.OpenTestDBAt(t, cfg.DBPath)
	engine := agentsync.NewEngine(t.Context(), database, agentsync.EngineConfig{
		AgentDirs: cfg.AgentDirs, Machine: cfg.InstallationID,
		DeferStartupMaintenance: true,
	})
	t.Cleanup(engine.Close)
	ts := httptest.NewUnstartedServer(nil)
	cfg.Host, cfg.Port = "127.0.0.1", ts.Listener.Addr().(*net.TCPAddr).Port
	srv := server.New(cfg, database, engine)
	startupCalls := 0
	ts.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/sync" {
			assert.Equal(t, "true", r.URL.Query().Get("startup_only"))
			startupCalls++
		}
		srv.Handler().ServeHTTP(w, r)
	})
	ts.Start()
	t.Cleanup(ts.Close)
	stubStartBackgroundServeForTransport(t, func(_ context.Context, launch *config.Config, _ time.Duration, _ bool) (*DaemonRuntime, error) {
		assert.True(t, launch.SkipInitialSync)
		registerTestRuntime(t, cfg.DataDir, ts.URL, false)
		return daemonRuntimeFromTestURL(t, ts.URL), nil
	})
	policy := archiveQueryPolicy{AutoStart: true, SkipInitialSync: true}
	daily, closeDaily, err := resolveArchiveQueryBackendWithConfig(t.Context(), cfg, policy)
	require.NoError(t, err)
	defer closeDaily()
	_, err = daily.DailyUsage(t.Context(), dailyUsageQuery{})
	require.NoError(t, err)
	assert.False(t, engine.StartupReconciled(), "daily usage must not force ingestion")
	assert.Zero(t, startupCalls)

	policy.SkipInitialSync = false
	fresh, closeFresh, err := resolveArchiveQueryBackendWithConfig(t.Context(), cfg, policy)
	require.NoError(t, err)
	defer closeFresh()
	_, err = fresh.DailyUsage(t.Context(), dailyUsageQuery{})
	require.NoError(t, err)
	session, err := database.GetSession(t.Context(), "session0")
	require.NoError(t, err)
	require.NotNil(t, session, "statusline must query after startup ingestion")
	lastSync := engine.LastSyncStartedAt(t.Context())
	_, cleanup, err := resolveArchiveQueryBackendWithConfig(t.Context(), cfg, policy)
	require.NoError(t, err)
	cleanup()
	assert.Equal(t, lastSync, engine.LastSyncStartedAt(t.Context()), "warm prompt refresh must not repeat a full sync")
}

func TestSessionUsageBeforeStartupImportsClaudeChild(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	claudeDir := filepath.Join(cfg.AgentDirs[parser.AgentClaude][0], "-home-proj0")
	database := dbtest.OpenTestDBAt(t, cfg.DBPath)
	const timestamp = "2026-01-01T00:00:00Z"
	parentPath := filepath.Join(claudeDir, "parent-a.jsonl")
	parent := testjsonl.NewSessionBuilder().
		AddClaudeUserWithSessionID(timestamp, "parent task", "parent-a").
		AddClaudeAssistantUsage(timestamp, "parent result", testjsonl.ClaudeAssistantUsage{OutputTokens: 10}).String()
	require.NoError(t, os.WriteFile(parentPath, []byte(parent), 0o600))
	childDir := filepath.Join(claudeDir, "parent-a", "subagents")
	require.NoError(t, os.MkdirAll(childDir, 0o700))
	child := testjsonl.NewSessionBuilder().
		AddClaudeUserWithSessionID(timestamp, "child task", "parent-a").
		AddClaudeAssistantUsage(timestamp, "child result", testjsonl.ClaudeAssistantUsage{OutputTokens: 42}).String()
	require.NoError(t, os.WriteFile(filepath.Join(childDir, "agent-child-a.jsonl"), []byte(child), 0o600))
	engine := agentsync.NewEngine(t.Context(), database, agentsync.EngineConfig{
		AgentDirs: cfg.AgentDirs, Machine: cfg.InstallationID, DeferStartupMaintenance: true,
	})
	t.Cleanup(engine.Close)
	_, err := service.NewDirectBackend(database, engine).Sync(t.Context(), service.SyncInput{Path: parentPath})
	require.NoError(t, err)
	require.False(t, engine.StartupReconciled())
	ts := httptest.NewUnstartedServer(nil)
	cfg.Host, cfg.Port = "127.0.0.1", ts.Listener.Addr().(*net.TCPAddr).Port
	srv := server.New(cfg, database, engine)
	ts.Config.Handler = srv.Handler()
	ts.Start()
	t.Cleanup(ts.Close)
	stubStartBackgroundServeForTransport(t, func(_ context.Context, _ *config.Config, _ time.Duration, _ bool) (*DaemonRuntime, error) {
		return daemonRuntimeFromTestURL(t, ts.URL), nil
	})
	backend, cleanup, err := resolveArchiveQueryBackendWithConfig(t.Context(), cfg, archiveQueryPolicy{
		AutoStart: true, SkipInitialSync: true,
	})
	require.NoError(t, err)
	defer cleanup()
	require.NoError(t, engine.RunExclusive(func() error {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		stderr := captureStderr(t, func() {
			out, code, err := backend.SessionUsage(ctx, sessionUsageQuery{SessionID: "parent-a"})
			require.NoError(t, err)
			require.NotNil(t, out)
			assert.Equal(t, tokenUseExitOK, code)
			assert.Equal(t, 10, out.TotalOutputTokens)
			assert.Zero(t, out.SubagentCount)
		})
		assert.Contains(t, stderr, "sync or maintenance is already running")
		return nil
	}))
	out, code, err := backend.SessionUsage(t.Context(), sessionUsageQuery{SessionID: "parent-a"})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, tokenUseExitOK, code)
	assert.Equal(t, 52, out.TotalOutputTokens)
	assert.Equal(t, 1, out.SubagentCount)
}

func TestUsageProgressRejectsDaemonWithoutStreamEndpoint(t *testing.T) {
	err := daemonRuntimeCompatibilityError(&DaemonRuntime{
		API: 8, Data: db.CurrentDataVersion(),
	})
	require.ErrorContains(t, err, "restart the daemon")
}

func writeUsageStreamResponse(t *testing.T, w http.ResponseWriter, r *http.Request, body string) {
	t.Helper()
	assert.Equal(t, "/api/v1/usage/summary/stream", r.URL.Path)
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprint(w, "event: done\ndata: ", strings.ReplaceAll(body, "\n", ""), "\n\n")
}

func TestFetchHTTPDailyUsageStreamsProgressAndResult(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/usage/summary/stream", r.URL.Path)
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: progress\ndata: {\"detail\":\"Preparing usage data for 2 sessions in this report\"}\n\n")
		http.NewResponseController(w).Flush()
		fmt.Fprint(w, "event: done\ndata: ", strings.ReplaceAll(sampleDailyUsageJSON, "\n", ""), "\n\n")
	}))
	t.Cleanup(ts.Close)
	var phases []string
	got, err := fetchHTTPDailyUsage(t.Context(), transport{URL: ts.URL}, "test-token", dailyUsageQuery{
		Progress: func(phase string) { phases = append(phases, phase) },
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"Preparing usage data for 2 sessions in this report"}, phases)
	require.Len(t, got.Daily, 1)
	assert.Equal(t, "2026-06-01", got.Daily[0].Date)
	assert.Equal(t, int64(420_000), got.Totals.TotalCost.Microdollars)
}

func TestFetchHTTPDailyUsageReportsStreamFailures(t *testing.T) {
	for _, tc := range []struct{ name, input, want string }{
		{"failed query", "event: error\ndata: {\"error\":\"could not read usage data\"}\n\n", "could not read usage data"},
		{"interrupted report", "event: progress\ndata: {\"detail\":\"Calculating daily totals\"}\n\n", "missing done event"},
		{"invalid progress", "event: progress\ndata: invalid\n\n", "decoding daemon push progress"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, tc.input)
			}))
			t.Cleanup(ts.Close)
			_, err := fetchHTTPDailyUsage(t.Context(), transport{URL: ts.URL}, "", dailyUsageQuery{
				Progress: func(string) {},
			})
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestUsageProgressPrintsSlowWorkToStderr(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var output syncBuffer
		writeProgress, finish := newUsageProgressPrinter(&output)
		defer finish()
		writeProgress("Reading archived sessions for this report")
		assert.Empty(t, output.String(), "a warm report should stay quiet")
		writeProgress("Preparing usage data for 2 sessions in this report")
		time.Sleep(time.Second)
		synctest.Wait()
		assert.Contains(t, output.String(), "Preparing usage data for 2 sessions in this report (1s)")
		time.Sleep(5 * time.Second)
		synctest.Wait()
		assert.Contains(t, output.String(), "(6s)", "long preparation must keep reporting its elapsed time")
	})
}

func usageHandlerBackend(t *testing.T, cfg config.Config, database *db.DB, engine *agentsync.Engine) daemonArchiveQueryBackend {
	t.Helper()
	ts := httptest.NewUnstartedServer(nil)
	cfg.Host, cfg.Port = "127.0.0.1", ts.Listener.Addr().(*net.TCPAddr).Port
	ts.Config.Handler = server.New(cfg, database, engine).Handler()
	ts.Start()
	t.Cleanup(ts.Close)
	return daemonArchiveQueryBackend{tr: transport{URL: ts.URL, Runtime: daemonRuntimeFromTestURL(t, ts.URL)}}
}

func TestSessionUsageOwnOnlyRealRefreshFailures(t *testing.T) {
	for _, failure := range []string{"busy", "write failure"} {
		t.Run(failure, func(t *testing.T) {
			cfg := testConfigWithClaudeFixture(t)
			database := dbtest.OpenTestDBAt(t, cfg.DBPath)
			engine := agentsync.NewEngine(t.Context(), database, agentsync.EngineConfig{AgentDirs: cfg.AgentDirs, Machine: cfg.InstallationID, DeferStartupMaintenance: true})
			t.Cleanup(engine.Close)
			path := filepath.Join(cfg.AgentDirs[parser.AgentClaude][0], "-home-proj0", "session0.jsonl")
			write := func(output int) {
				require.NoError(t, os.WriteFile(path, []byte(testjsonl.NewSessionBuilder().
					AddClaudeUser("2026-01-01T00:00:00Z", "task").
					AddClaudeAssistantUsage("2026-01-01T00:00:01Z", "answer", testjsonl.ClaudeAssistantUsage{OutputTokens: output}).String()), 0o600))
			}
			write(10)
			require.NoError(t, engine.SyncPathsContext(t.Context(), []string{path}))
			write(99)
			backend := usageHandlerBackend(t, cfg, database, engine)
			if failure == "write failure" {
				conn, err := sql.Open("sqlite3", cfg.DBPath)
				require.NoError(t, err)
				_, err = conn.ExecContext(t.Context(), `CREATE TRIGGER reject_refresh BEFORE INSERT ON sessions BEGIN SELECT RAISE(ABORT, 'refresh write rejected'); END`)
				require.NoError(t, err)
				require.NoError(t, conn.Close())
			}
			read := func() error {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				start := time.Now()
				stderr := captureStderr(t, func() {
					out, code, err := backend.SessionUsage(ctx, sessionUsageQuery{SessionID: "session0", OwnOnly: true})
					require.NoError(t, err)
					require.NotNil(t, out)
					assert.Equal(t, tokenUseExitOK, code)
					assert.Equal(t, 10, out.TotalOutputTokens)
				})
				assert.Contains(t, stderr, "showing stored usage")
				if failure == "busy" {
					assert.Contains(t, stderr, "HTTP 409")
					assert.Contains(t, stderr, "sync or maintenance is already running")
					assert.Less(t, time.Since(start), 2*time.Second)
				} else {
					assert.Contains(t, stderr, "refresh write rejected")
				}
				return nil
			}
			if failure == "busy" {
				require.NoError(t, engine.RunExclusive(read))
			} else {
				require.NoError(t, read())
			}
		})
	}
}

func TestSessionUsageVibeReplacementID(t *testing.T) {
	for _, promotion := range []bool{false, true} {
		for _, ownOnly := range []bool{false, true} {
			for _, direct := range []bool{false, true} {
				t.Run(fmt.Sprintf("promotion=%t/own_only=%t/direct=%t", promotion, ownOnly, direct), func(t *testing.T) {
					root := t.TempDir()
					cfg := config.Config{DBPath: filepath.Join(t.TempDir(), "sessions.db"), AgentDirs: map[parser.AgentType][]string{parser.AgentVibe: {root}}, InstallationID: "local"}
					database := dbtest.OpenTestDBAt(t, cfg.DBPath)
					engine := agentsync.NewEngine(t.Context(), database, agentsync.EngineConfig{AgentDirs: cfg.AgentDirs, Machine: cfg.InstallationID, DeferStartupMaintenance: true})
					t.Cleanup(engine.Close)
					const dirName = "session_20260616_083518_abc123"
					const canonical = "vibe:11111111-1111-4111-8111-111111111111"
					dir := filepath.Join(root, dirName)
					require.NoError(t, os.MkdirAll(dir, 0o700))
					messages := filepath.Join(dir, "messages.jsonl")
					require.NoError(t, os.WriteFile(messages, []byte(`{"role":"user","content":"hello"}`+"\n"), 0o600))
					meta := filepath.Join(dir, "meta.json")
					writeMeta := func() {
						require.NoError(t, os.WriteFile(meta, []byte(`{"session_id":"11111111-1111-4111-8111-111111111111"}`), 0o600))
					}
					if !promotion {
						writeMeta()
					}
					require.NoError(t, engine.SyncPathsContext(t.Context(), []string{messages}))
					requested, expected := canonical, "vibe:"+dirName
					if promotion {
						writeMeta()
						requested, expected = expected, requested
					} else {
						require.NoError(t, os.Remove(meta))
					}
					var backend archiveQueryBackend
					if direct {
						backend = localArchiveQueryBackend{cfg: cfg, database: database, offline: true}
					} else {
						backend = usageHandlerBackend(t, cfg, database, engine)
					}
					stderr := captureStderr(t, func() {
						out, code, err := backend.SessionUsage(t.Context(), sessionUsageQuery{SessionID: requested, OwnOnly: ownOnly})
						require.NoError(t, err)
						require.NotNil(t, out)
						assert.Equal(t, expected, out.SessionID)
						assert.Equal(t, tokenUseExitNoTokenData, code)
					})
					assert.Empty(t, stderr)
					old, err := database.GetSession(t.Context(), requested)
					require.NoError(t, err)
					assert.Nil(t, old)
				})
			}
		}
	}
}

func TestSessionUsageColdStartRefreshesCodexChild(t *testing.T) {
	for _, agent := range []parser.AgentType{parser.AgentCodex, parser.AgentTraeX, parser.AgentAugureCode} {
		for _, state := range []string{"archived", "new child", "new parent", "missing intermediate", "metadata spawn", "imported grandchild", "restored child", "missing spawner"} {
			t.Run(string(agent)+"/"+state, func(t *testing.T) {
				def, ok := parser.AgentByType(agent)
				require.True(t, ok)
				id := func(raw string) string { return def.IDPrefix + raw }
				root := t.TempDir()
				cfg := config.Config{DBPath: filepath.Join(t.TempDir(), "sessions.db"), AgentDirs: map[parser.AgentType][]string{agent: {root}}, InstallationID: "local"}
				database := dbtest.OpenTestDBAt(t, cfg.DBPath)
				engineConfig := agentsync.EngineConfig{AgentDirs: cfg.AgentDirs, Machine: cfg.InstallationID, DeferStartupMaintenance: true}
				engine := agentsync.NewEngine(t.Context(), database, engineConfig)
				const parent = "11111111-1111-4111-8111-111111111111"
				const child = "22222222-2222-4222-8222-222222222222"
				const other = "33333333-3333-4333-8333-333333333333"
				const grandchild = "44444444-4444-4444-8444-444444444444"
				const timestamp = "2026-01-01T00:00:00Z"
				write := func(id, metadata string, output int, spawned string) string {
					path := filepath.Join(root, "rollout-2026-01-01T00-00-00-"+id+".jsonl")
					lines := []string{
						metadata, testjsonl.CodexTurnContextJSON("gpt-5", timestamp),
						testjsonl.CodexMsgJSON("user", "task", timestamp),
						testjsonl.CodexMsgJSON("assistant", "answer", timestamp),
						testjsonl.CodexTokenCountJSON(timestamp, 20, output, 0),
					}
					if spawned != "" {
						lines = append(lines, testjsonl.CodexFunctionCallWithCallIDJSON("spawn_agent", "spawn", map[string]any{"message": "child task"}, timestamp),
							testjsonl.CodexFunctionCallOutputJSON("spawn", `{"agent_id":"`+spawned+`"}`, timestamp))
					}
					require.NoError(t, os.WriteFile(path, []byte(testjsonl.JoinJSONL(lines...)), 0o600))
					return path
				}
				parentMeta := testjsonl.CodexSessionMetaJSON(parent, "/work/project", "user", timestamp)
				parentPath := write(parent, parentMeta, 10, "")
				childMeta := testjsonl.CodexSubagentSessionMetaJSON(child, parent, "/work/project", "user", timestamp)
				childPath := write(child, childMeta, 1, "")
				paths := []string{parentPath}
				if state != "new child" && state != "new parent" && state != "imported grandchild" {
					paths = append(paths, childPath)
				}
				grandchildMeta := testjsonl.CodexSubagentSessionMetaJSON(grandchild, child, "/work/project", "user", timestamp)
				if state == "missing spawner" {
					write(child, childMeta, 1, grandchild)
				}
				if state == "missing intermediate" || state == "imported grandchild" {
					paths = append(paths, write(grandchild, grandchildMeta, 2, ""))
				}
				if state != "new parent" {
					require.NoError(t, engine.SyncPathsContext(t.Context(), paths))
				}
				engine.Close()
				if state == "new child" || state == "new parent" || state == "imported grandchild" {
					absent, err := database.GetSession(t.Context(), id(child))
					require.NoError(t, err)
					require.Nil(t, absent)
					write(parent, parentMeta, 10, child)
				}
				wantOutput, wantChildren := 52, 1
				if state == "missing intermediate" || state == "restored child" || state == "missing spawner" {
					require.NoError(t, database.BaselineActiveSessionSourceOwnerships(t.Context(), []db.SessionSourceOwnership{{ID: id(child), Machine: "local", Agent: string(agent), FilePath: childPath}}))
					marked, err := database.MarkSessionSourceMissing(t.Context(), "local", string(agent), id(child), childPath)
					require.NoError(t, err)
					require.True(t, marked)
					require.NoError(t, os.Remove(childPath))
					if state == "restored child" {
						write(child, childMeta, 42, "")
					} else {
						write(grandchild, grandchildMeta, 42, "")
						wantOutput, wantChildren = 53, 2
					}
				} else if state == "metadata spawn" || state == "imported grandchild" {
					write(child, childMeta, 1, grandchild)
					write(grandchild, grandchildMeta, 42, "")
					wantOutput, wantChildren = 53, 2
				} else {
					write(child, childMeta, 42, "")
				}
				write(other, testjsonl.CodexSessionMetaJSON(other, "/work/project", "user", timestamp), 999, "")
				engine = agentsync.NewEngine(t.Context(), database, engineConfig)
				t.Cleanup(engine.Close)
				var backend archiveQueryBackend = usageHandlerBackend(t, cfg, database, engine)
				if state == "new parent" {
					backend = localArchiveQueryBackend{cfg: cfg, database: database, offline: true}
				}
				require.False(t, engine.StartupReconciled())
				stderr := captureStderr(t, func() {
					out, code, err := backend.SessionUsage(t.Context(), sessionUsageQuery{SessionID: id(parent)})
					require.NoError(t, err)
					require.NotNil(t, out)
					assert.Equal(t, tokenUseExitOK, code)
					assert.Equal(t, wantOutput, out.TotalOutputTokens)
					assert.Equal(t, wantChildren, out.SubagentCount)
				})
				assert.Empty(t, stderr)
				assert.False(t, engine.StartupReconciled())
				unrelated, err := database.GetSession(t.Context(), id(other))
				require.NoError(t, err)
				assert.Nil(t, unrelated)
			})
		}
	}
}
