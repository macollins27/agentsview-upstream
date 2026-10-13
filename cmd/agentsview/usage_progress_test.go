package main

import (
	"context"
	"encoding/json/v2"
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

func TestSessionUsageVibeReplacementID(t *testing.T) {
	for _, ownOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("own_only=%t", ownOnly), func(t *testing.T) {
			const requested = "vibe:old-session"
			const replacement = "vibe:replacement-session"
			var paths []string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paths = append(paths, r.URL.Path)
				switch r.URL.Path {
				case "/api/v1/sessions/" + requested:
					writeJSONResponse(w, `{"id":"`+requested+`","agent":"vibe"}`)
				case "/api/v1/sessions/sync":
					var input service.SyncInput
					if !assert.NoError(t, json.UnmarshalRead(r.Body, &input)) {
						return
					}
					assert.Equal(t, service.SyncInput{ID: requested, Subagents: !ownOnly, UsageRefresh: true}, input)
					writeJSONResponse(w, `{"id":"`+replacement+`"}`)
				case "/api/v1/sessions/" + replacement + "/usage":
					query := "breakdown=true"
					if !ownOnly {
						query += "&subagents=true"
					}
					assert.Equal(t, query, r.URL.RawQuery)
					writeJSONResponse(w, `{"session_id":"`+replacement+`","agent":"vibe","has_token_data":false}`)
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(ts.Close)
			backend := daemonArchiveQueryBackend{tr: transport{URL: ts.URL, Runtime: daemonRuntimeFromTestURL(t, ts.URL)}}
			stderr := captureStderr(t, func() {
				out, code, err := backend.SessionUsage(t.Context(), sessionUsageQuery{SessionID: requested, OwnOnly: ownOnly})
				require.NoError(t, err)
				require.NotNil(t, out)
				assert.Equal(t, replacement, out.SessionID)
				assert.Equal(t, tokenUseExitNoTokenData, code)
			})
			assert.Empty(t, stderr)
			assert.Equal(t, []string{"/api/v1/sessions/" + requested, "/api/v1/sessions/sync", "/api/v1/sessions/" + replacement + "/usage"}, paths)
		})
	}
	t.Run("local", func(t *testing.T) {
		for _, promotion := range []bool{false, true} {
			for _, ownOnly := range []bool{false, true} {
				t.Run(fmt.Sprintf("promotion=%t/own_only=%t", promotion, ownOnly), func(t *testing.T) {
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
					backend := localArchiveQueryBackend{cfg: cfg, database: database, offline: true}
					stderr := captureStderr(t, func() {
						out, code, err := backend.SessionUsage(t.Context(), sessionUsageQuery{SessionID: requested, OwnOnly: ownOnly})
						require.NoError(t, err)
						require.NotNil(t, out)
						assert.Equal(t, expected, out.SessionID)
						assert.Equal(t, tokenUseExitNoTokenData, code)
					})
					assert.Empty(t, stderr)
				})
			}
		}
	})
}
