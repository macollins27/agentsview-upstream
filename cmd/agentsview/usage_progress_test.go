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
	"strconv"
	"strings"
	"sync"
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
	"go.kenn.io/agentsview/internal/servicehttp"
	agentsync "go.kenn.io/agentsview/internal/sync"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestUsageCommandsDeferStartupSyncExceptStatusline(t *testing.T) {
	for _, command := range []string{"daily", "statusline", "session usage", "token-use"} {
		t.Run(command, func(t *testing.T) {
			newAgentDataDir(t)
			var ts *httptest.Server
			var requests *remoteUsageRequests
			if command == "daily" || command == "statusline" {
				ts = sessionUsageRuntimeServer(t, func(w http.ResponseWriter, r *http.Request) {
					if command == "daily" {
						writeUsageStreamResponse(t, w, r, sampleDailyUsageJSON)
					} else {
						writeJSONResponse(w, sampleDailyUsageJSON)
					}
				})
			} else {
				ts, requests = newRemoteUsageServer(t, remoteUsageSpec{canonicalID: "codex:session-a"})
			}
			started := false
			var statuslineDeadline time.Time
			stubStartBackgroundServeForTransport(t, func(ctx context.Context, cfg *config.Config, _ time.Duration, _ bool) (*DaemonRuntime, error) {
				started = true
				if command == "statusline" {
					deadline, ok := ctx.Deadline()
					assert.True(t, ok, "prompt refresh must have a deadline")
					if ok {
						statuslineDeadline = deadline
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
					before := time.Now()
					runUsageStatusline(UsageStatuslineConfig{JSON: true})
					assert.WithinRange(t, statuslineDeadline, before.Add(30*time.Second), time.Now().Add(30*time.Second))
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
			if requests != nil {
				assert.NotContains(t, requests.RequestPath, "/api/v1/sync")
			}
		})
	}
}

func TestUsageCommandsReconcileDaemonStartedByDaily(t *testing.T) {
	for _, sessionQuery := range []bool{false, true} {
		t.Run(fmt.Sprintf("session=%t", sessionQuery), func(t *testing.T) {
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
			ts.Config.Handler = srv.Handler()
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

			policy.SkipInitialSync = sessionQuery
			fresh, closeFresh, err := resolveArchiveQueryBackendWithConfig(t.Context(), cfg, policy)
			require.NoError(t, err)
			defer closeFresh()
			if sessionQuery {
				out, _, err := fresh.SessionUsage(t.Context(), sessionUsageQuery{SessionID: "session0", OwnOnly: true})
				require.NoError(t, err)
				require.NotNil(t, out, "lookup must happen after the missing session is imported")
				assert.False(t, engine.StartupReconciled())
			} else {
				_, err := fresh.DailyUsage(t.Context(), dailyUsageQuery{})
				require.NoError(t, err)
				session, err := database.GetSession(t.Context(), "session0")
				require.NoError(t, err)
				require.NotNil(t, session, "statusline must query after startup ingestion")
			}
			lastSync := engine.LastSyncStartedAt(t.Context())
			_, cleanup, err := resolveArchiveQueryBackendWithConfig(t.Context(), cfg, policy)
			require.NoError(t, err)
			cleanup()
			assert.Equal(t, lastSync, engine.LastSyncStartedAt(t.Context()), "warm prompt refresh must not repeat a full sync")
		})
	}
}

func TestDaemonSessionUsageImportsOnlyRequestedSource(t *testing.T) {
	const rawID = "a1b2c3d4-1111-4222-8333-444455556666"
	for _, tc := range []struct {
		id         string
		ownOnly    bool
		noSync     bool
		freebuff   bool
		archived   bool
		stale      bool
		direct     bool
		busy       bool
		vibe       int
		unarchived bool
	}{
		{id: "codex:" + rawID, stale: true, direct: true},
		{id: "codex:" + rawID, stale: true, archived: true, direct: true},
		{id: "codex:" + rawID, busy: true, ownOnly: true},
		{id: "codex:" + rawID, busy: true, archived: true, ownOnly: true},
		{id: "codex:" + rawID, busy: true, archived: true},
		{id: "codex:" + rawID, stale: true},
		{id: "vibe:abc123def-0000-0000-0000-000000000000", ownOnly: true, vibe: 2},
		{id: "vibe:session_20260616_083518_abc123", vibe: 1},
		{id: "vibe:session_20260616_083518_abc123", vibe: 1, unarchived: true},
		{id: "missing-session"},
		{id: "codex:" + rawID},
		{id: "codex:" + rawID, ownOnly: true},
		{id: "freebuff:project-a:2026-07-15T20-01-32.065Z", freebuff: true},
		{id: "codex:" + rawID, ownOnly: true, archived: true},
		{id: "codex:" + rawID, ownOnly: true, archived: true, noSync: true},
	} {
		t.Run(fmt.Sprintf("%s/own=%t/no-sync=%t", tc.id, tc.ownOnly, tc.noSync), func(t *testing.T) {
			cfg := testConfigWithClaudeFixture(t)
			root := t.TempDir()
			cfg.AgentDirs[parser.AgentCodex] = []string{root}
			content := testjsonl.NewSessionBuilder().
				AddCodexMeta("2026-05-04T14:00:00Z", rawID, "/home/user/code/api", "codex_cli_rs").
				AddCodexMessage("2026-05-04T14:00:01Z", "user", "hello").String()
			require.NoError(t, os.WriteFile(filepath.Join(root, "rollout-2026-05-04T14-00-00-"+rawID+".jsonl"), []byte(content), 0o644))
			expectedID := "codex:" + rawID
			if tc.freebuff {
				cfg.AgentDirs[parser.AgentCodebuff] = []string{root}
				dir := filepath.Join(root, "project-a", "chats", "2026-07-15T20-01-32.065Z")
				require.NoError(t, os.MkdirAll(dir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "chat-messages.json"), []byte(`[{"id":"user-1","variant":"user","content":"Hello","timestamp":"10:00 AM"}]`), 0o644))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "run-state.json"), []byte(`{"sessionState":{"mainAgentState":{"agentType":"base2-free-minimax-m3"}}}`), 0o644))
				expectedID = tc.id
			}
			var vibePath, metaPath string
			if tc.vibe != 0 {
				vibeRoot := t.TempDir()
				cfg.AgentDirs[parser.AgentVibe] = []string{vibeRoot}
				dir := filepath.Join(vibeRoot, "session_20260616_083518_abc123")
				require.NoError(t, os.MkdirAll(dir, 0o755))
				vibePath, metaPath = filepath.Join(dir, "messages.jsonl"), filepath.Join(dir, "meta.json")
				require.NoError(t, os.WriteFile(vibePath, []byte(`{"role":"user","content":"hello vibe"}`+"\n"), 0o644))
				if tc.vibe == 2 {
					require.NoError(t, os.WriteFile(metaPath, []byte(`{"session_id":"abc123def-0000-0000-0000-000000000000"}`), 0o644))
				}
			}
			database := dbtest.OpenTestDBAt(t, cfg.DBPath)
			engine := agentsync.NewEngine(t.Context(), database, agentsync.EngineConfig{
				AgentDirs: cfg.AgentDirs, Machine: cfg.InstallationID, DeferStartupMaintenance: true,
			})
			t.Cleanup(engine.Close)
			if tc.vibe != 0 {
				if !tc.unarchived {
					require.NoError(t, engine.SyncPathsContext(t.Context(), []string{vibePath}))
				}
				if tc.vibe == 1 {
					require.NoError(t, os.WriteFile(metaPath, []byte(`{"session_id":"abc123def-0000-0000-0000-000000000000"}`), 0o644))
					expectedID = "vibe:abc123def-0000-0000-0000-000000000000"
				} else {
					require.NoError(t, os.Remove(metaPath))
					expectedID = "vibe:session_20260616_083518_abc123"
				}
				future := time.Now().Add(time.Hour)
				require.NoError(t, os.Chtimes(vibePath, future, future))
			}
			if tc.archived {
				require.NoError(t, engine.SyncSingleSessionContext(t.Context(), expectedID))
				content += "\n" + testjsonl.NewSessionBuilder().AddCodexMessage("2026-05-04T14:00:02Z", "assistant", "updated").String()
				require.NoError(t, os.WriteFile(filepath.Join(root, "rollout-2026-05-04T14-00-00-"+rawID+".jsonl"), []byte(content), 0o644))
			}
			var options []server.Option
			if tc.stale {
				engine.Close()
				require.NoError(t, database.Close())
				raw, err := sql.Open("sqlite3", cfg.DBPath)
				require.NoError(t, err)
				_, err = raw.ExecContext(t.Context(), "PRAGMA user_version = 0")
				require.NoError(t, err)
				require.NoError(t, raw.Close())
				database, err = db.Open(t.Context(), cfg.DBPath)
				require.NoError(t, err)
				t.Cleanup(func() { database.Close() })
				engine = agentsync.NewEngine(t.Context(), database, agentsync.EngineConfig{AgentDirs: cfg.AgentDirs, DeferStartupMaintenance: true})
				t.Cleanup(engine.Close)
				if !tc.direct {
					options = append(options, server.WithLocalSyncRunner(func(context.Context, func(agentsync.Progress)) (agentsync.SyncStats, error) {
						assert.Fail(t, "session usage must not start global sync")
						return agentsync.SyncStats{}, nil
					}), server.WithLocalResyncRunner(func(context.Context, func(agentsync.Progress)) (agentsync.SyncStats, error) {
						assert.Fail(t, "session usage must not start archive rebuild")
						return agentsync.SyncStats{}, nil
					}))
				}
			}
			var releaseWriter func()
			var manualDone chan struct{}
			var manualErr error
			if tc.busy {
				entered, release := make(chan struct{}), make(chan struct{})
				done := make(chan error, 1)
				go func() { done <- engine.RunExclusive(func() error { close(entered); <-release; return nil }) }()
				<-entered
				var once sync.Once
				releaseWriter = func() { once.Do(func() { close(release) }) }
				defer func() {
					releaseWriter()
					require.NoError(t, <-done)
					if manualDone != nil {
						<-manualDone
						require.NoError(t, manualErr)
					}
				}()
			}
			ts := httptest.NewUnstartedServer(nil)
			cfg.Host, cfg.Port = "127.0.0.1", ts.Listener.Addr().(*net.TCPAddr).Port
			srv := server.New(cfg, database, engine, options...)
			ts.Config.Handler = srv.Handler()
			ts.Start()
			t.Cleanup(ts.Close)
			if tc.busy && tc.archived && tc.ownOnly {
				manualDone = make(chan struct{})
				go func() {
					_, manualErr = servicehttp.NewHTTPBackend(ts.URL, "", false, "").Sync(t.Context(), service.SyncInput{ID: expectedID})
					close(manualDone)
				}()
			}
			backend := daemonArchiveQueryBackend{tr: transport{URL: ts.URL}}
			var out *sessionUsageOutput
			var code int
			var err error
			stderr := captureStderr(t, func() {
				done := make(chan struct{})
				go func() {
					query := sessionUsageQuery{SessionID: tc.id, OwnOnly: tc.ownOnly, NoSync: tc.noSync}
					if tc.direct {
						out, code, err = (localArchiveQueryBackend{database: database, cfg: cfg}).SessionUsage(t.Context(), query)
					} else {
						out, code, err = backend.SessionUsage(t.Context(), query)
					}
					close(done)
				}()
				if tc.busy && !tc.archived {
					assert.Never(t, func() bool {
						select {
						case <-done:
							return true
						default:
							return false
						}
					}, 100*time.Millisecond, 10*time.Millisecond)
					releaseWriter()
				}
				select {
				case <-done:
				case <-time.After(30 * time.Second):
					require.FailNow(t, "usage waited for an unrelated sync")
				}
			})
			if manualDone != nil {
				assert.Never(t, func() bool {
					select {
					case <-manualDone:
						return true
					default:
						return false
					}
				}, 100*time.Millisecond, 10*time.Millisecond, "manual sync must wait for the writer")
			}
			if tc.stale && !tc.archived {
				require.ErrorContains(t, err, "agentsview resync")
				assert.Equal(t, tokenUseExitErr, code)
				assert.NotContains(t, stderr, "session not found")
				return
			}
			require.NoError(t, err)
			if tc.id == "missing-session" {
				assert.Equal(t, tokenUseExitNotFound, code)
				assert.Nil(t, out)
				assert.Contains(t, stderr, "session not found: missing-session")
				return
			}
			if tc.stale || (tc.busy && tc.archived) {
				assert.Contains(t, stderr, "warning: refresh incomplete; showing stored usage:")
			}
			if tc.noSync && !tc.archived {
				assert.Nil(t, out)
				assert.Equal(t, tokenUseExitNotFound, code)
			} else {
				require.NotNil(t, out, "refresh stderr: %s", stderr)
				assert.Equal(t, expectedID, out.SessionID)
			}
			if tc.archived {
				session, err := database.GetSession(t.Context(), expectedID)
				require.NoError(t, err)
				require.NotNil(t, session)
				if tc.noSync || tc.stale || tc.busy {
					assert.Equal(t, 1, session.MessageCount)
				} else {
					assert.Equal(t, 2, session.MessageCount)
				}
			}
			assert.False(t, engine.StartupReconciled())
			if tc.id == "codex:"+rawID && !tc.ownOnly && !tc.busy && !tc.stale {
				unrelated, err := database.GetSession(t.Context(), "session0")
				require.NoError(t, err)
				assert.Nil(t, unrelated)
			}
		})
	}
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

func TestDaemonSessionUsageImportsConfiguredS3Source(t *testing.T) {
	const rawID = "a1b2c3d4-1111-4222-8333-444455556666"
	for _, tc := range []struct {
		id             string
		codex, ownOnly bool
	}{
		{"laptop~shared-id", false, false},
		{"shared-id", false, true},
		{"desktop~shared-id", false, true},
		{"codex:" + rawID, true, false},
	} {
		t.Run(tc.id, func(t *testing.T) {
			cfg := testConfigWithClaudeFixture(t)
			agent, provider, filename := parser.AgentClaude, "claude", "shared-id.jsonl"
			body := testjsonl.NewSessionBuilder().AddClaudeUser("2026-05-04T14:00:00Z", "hello").AddClaudeAssistant("2026-05-04T14:00:01Z", "done").String()
			if tc.codex {
				agent, provider, filename = parser.AgentCodex, "codex", "rollout-2026-05-04T14-00-00-"+rawID+".jsonl"
				body = testjsonl.NewSessionBuilder().AddCodexMeta("2026-05-04T14:00:00Z", rawID, "/repo", "codex").AddCodexMessage("2026-05-04T14:00:01Z", "user", "hello").String()
			}
			roots := []string{"s3://bucket/laptop/raw/" + provider, "s3://bucket/desktop/raw/" + provider}
			cfg.AgentDirs = map[parser.AgentType][]string{agent: roots}
			objects := map[string]string{}
			for _, root := range roots {
				objects[strings.TrimPrefix(root, "s3://bucket/")+"/project/"+filename] = body
				objects[strings.TrimPrefix(root, "s3://bucket/")+"/project/unrelated.jsonl"] = body
			}
			var mu sync.Mutex
			fetched := []string{}
			s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Has("location") {
					fmt.Fprint(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
					return
				}
				if r.URL.Query().Has("list-type") || r.URL.Query().Has("prefix") {
					fmt.Fprint(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><IsTruncated>false</IsTruncated>`)
					for key, value := range objects {
						if strings.HasPrefix(key, r.URL.Query().Get("prefix")) {
							fmt.Fprintf(w, `<Contents><Key>%s</Key><LastModified>2026-05-04T14:00:00Z</LastModified><ETag>"fixture"</ETag><Size>%d</Size></Contents>`, key, len(value))
						}
					}
					fmt.Fprint(w, `</ListBucketResult>`)
					return
				}
				key := strings.TrimPrefix(r.URL.Path, "/bucket/")
				value, ok := objects[key]
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					fmt.Fprint(w, `<Error><Code>NoSuchKey</Code></Error>`)
					return
				}
				w.Header().Set("Content-Length", strconv.Itoa(len(value)))
				w.Header().Set("Last-Modified", "Mon, 04 May 2026 14:00:00 GMT")
				w.Header().Set("ETag", `"fixture"`)
				if r.Method == http.MethodGet {
					mu.Lock()
					fetched = append(fetched, key)
					mu.Unlock()
					fmt.Fprint(w, value)
				}
			}))
			t.Cleanup(s3.Close)
			t.Setenv("AWS_S3_ENDPOINT", s3.URL)
			t.Setenv("AWS_ACCESS_KEY_ID", "fixture")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "fixture")
			database := dbtest.OpenTestDBAt(t, cfg.DBPath)
			engine := agentsync.NewEngine(t.Context(), database, agentsync.EngineConfig{AgentDirs: cfg.AgentDirs, Machine: cfg.InstallationID, DeferStartupMaintenance: true})
			t.Cleanup(engine.Close)
			ts := httptest.NewUnstartedServer(nil)
			cfg.Host, cfg.Port = "127.0.0.1", ts.Listener.Addr().(*net.TCPAddr).Port
			srv := server.New(cfg, database, engine, server.WithLocalSyncRunner(func(context.Context, func(agentsync.Progress)) (agentsync.SyncStats, error) {
				assert.Fail(t, "usage requested full sync")
				return agentsync.SyncStats{}, nil
			}))
			ts.Config.Handler = srv.Handler()
			ts.Start()
			t.Cleanup(ts.Close)
			out, _, err := (daemonArchiveQueryBackend{tr: transport{URL: ts.URL}}).SessionUsage(t.Context(), sessionUsageQuery{SessionID: tc.id, OwnOnly: tc.ownOnly})
			require.NoError(t, err)
			mu.Lock()
			defer mu.Unlock()
			require.NotNil(t, out)
			machine, _ := parser.StripHostPrefix(out.SessionID)
			assert.Contains(t, []string{"laptop", "desktop"}, machine)
			if host, _ := parser.StripHostPrefix(tc.id); host != "" {
				assert.Equal(t, host, machine)
			}
			expected := machine + "~shared-id"
			if tc.codex {
				expected = machine + "~codex:" + rawID
			}
			assert.Equal(t, expected, out.SessionID)
			require.NotEmpty(t, fetched)
			for _, key := range fetched {
				assert.Equal(t, machine+"/raw/"+provider+"/project/"+filename, key)
			}
			assert.False(t, engine.StartupReconciled())
		})
	}
}
