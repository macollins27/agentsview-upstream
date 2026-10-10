package sync

import (
	"context"
	"errors"
	"strings"

	"go.kenn.io/agentsview/internal/parser"
)

var subagentTranscriptPaths = parser.ClaudeSubagentTranscriptPaths

// SyncSessionWithSubagentsContext refreshes one session and the candidate
// Claude subagent transcripts in its root tree. Child discovery still runs
// when the requested-session refresh fails so callers can retain archived data
// while ingesting any descendants that remain available.
func (e *Engine) SyncSessionWithSubagentsContext(
	ctx context.Context,
	sessionID string,
) error {
	return e.syncSessionWithSubagentsContext(ctx, sessionID, false)
}

// TrySyncSessionWithSubagentsContext tries the writer for each requested source batch.
func (e *Engine) TrySyncSessionWithSubagentsContext(ctx context.Context, sessionID string) error {
	return e.syncSessionWithSubagentsContext(ctx, sessionID, true)
}

func (e *Engine) syncSessionWithSubagentsContext(ctx context.Context, sessionID string, foreground bool) error {
	parentErr := e.syncSingleSessionContext(ctx, sessionID, foreground)
	if errors.Is(parentErr, ErrSyncInProgress) {
		return parentErr
	}

	// The archive session ID carries its agent prefix (host prefixes are
	// stripped first), so the parent agent resolves without a row lookup and
	// stays correct even when the parent is soft-deleted.
	parentAgent := parser.AgentClaude
	if def, ok := parser.AgentByPrefix(sessionID); ok {
		parentAgent = def.Type
	}
	sourcePath := e.db.GetSessionFilePath(ctx, sessionID)
	if sourcePath == "" {
		sourcePath = e.FindSourceFile(ctx, sessionID)
	}
	paths := subagentTranscriptPaths(sourcePath)
	if len(paths) == 0 {
		return parentErr
	}

	var subagentErr error
	if strings.HasPrefix(sourcePath, "s3://") {
		subagentErr = e.syncS3SubagentTranscriptsContext(
			ctx, sessionID, parentAgent, paths, foreground)
	} else {
		subagentErr = e.syncPathsContext(ctx, paths, false, foreground)
	}
	return errors.Join(parentErr, subagentErr)
}
