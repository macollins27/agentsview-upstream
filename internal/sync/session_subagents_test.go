package sync

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
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
