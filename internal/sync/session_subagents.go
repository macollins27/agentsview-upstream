package sync

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.kenn.io/agentsview/internal/parser"
)

var subagentTranscriptPaths = parser.ClaudeSubagentTranscriptPaths

// ErrSubagentRefreshIncomplete marks lock contention after the parent refresh.
var ErrSubagentRefreshIncomplete = errors.New(
	"subagent refresh incomplete; usage aggregate may be incomplete",
)

// SyncSessionWithSubagentsContext refreshes one session and the candidate
// Claude subagent transcripts in its root tree. Child discovery still runs
// when the requested-session refresh fails so callers can retain archived data
// while ingesting any descendants that remain available.
func (e *Engine) SyncSessionWithSubagentsContext(
	ctx context.Context,
	sessionID string,
) error {
	parentErr := e.SyncSingleSessionForUsageContext(ctx, sessionID)
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
		subagentErr = e.SyncS3SubagentTranscriptsContext(
			ctx, sessionID, parentAgent, paths)
	} else {
		subagentErr = e.syncPathsContext(ctx, paths, false, false)
	}
	if errors.Is(subagentErr, ErrSyncInProgress) {
		subagentErr = fmt.Errorf("%w: %w", ErrSubagentRefreshIncomplete, subagentErr)
	}
	return errors.Join(parentErr, subagentErr)
}
