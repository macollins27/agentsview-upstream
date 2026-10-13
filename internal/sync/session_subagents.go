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

// SyncSessionWithSubagentsContext refreshes one session, its archived descendant
// sources, Claude subagent transcripts, and children named by Codex spawn records.
// Child discovery runs
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
	parent, lookupErr := e.db.GetSession(ctx, sessionID)
	if lookupErr != nil {
		return errors.Join(parentErr, lookupErr)
	}
	if parent != nil {
		descendantPaths, err := e.db.ListActiveDescendantSessionSourcePaths(ctx, parent.Machine, parent.Agent, []string{sessionID})
		if err != nil {
			return errors.Join(parentErr, err)
		}
		paths = append(paths, descendantPaths...)
	}
	seen := map[string]bool{sourcePath: true}
	unique := paths[:0]
	for _, path := range paths {
		if !seen[path] {
			seen[path] = true
			unique = append(unique, path)
		}
	}
	paths = unique
	var subagentErr error
	if len(paths) > 0 {
		if strings.HasPrefix(sourcePath, "s3://") {
			subagentErr = e.SyncS3SubagentTranscriptsContext(
				ctx, sessionID, parentAgent, paths)
		} else {
			subagentErr = e.syncPathsContext(ctx, paths, false, false)
		}
	}
	if parentAgent == parser.AgentCodex {
		subagentErr = errors.Join(subagentErr, e.syncCodexSpawnChildren(ctx, sessionID))
	}
	if errors.Is(subagentErr, ErrSyncInProgress) {
		subagentErr = fmt.Errorf("%w: %w", ErrSubagentRefreshIncomplete, subagentErr)
	}
	return errors.Join(parentErr, subagentErr)
}

func (e *Engine) syncCodexSpawnChildren(ctx context.Context, sessionID string) error {
	seen := map[string]bool{sessionID: true}
	parents := []string{sessionID}
	var refreshErr error
	for len(parents) > 0 {
		children, err := e.db.SubagentChildSessionIDs(ctx, parents)
		if err != nil {
			return errors.Join(refreshErr, err)
		}
		parents = nil
		for _, childID := range children {
			if seen[childID] {
				continue
			}
			seen[childID] = true
			child, err := e.db.GetSession(ctx, childID)
			if err != nil {
				return errors.Join(refreshErr, err)
			}
			if child == nil {
				refreshErr = errors.Join(refreshErr, e.SyncSingleSessionForUsageContext(ctx, childID))
			}
			parents = append(parents, childID)
		}
	}
	return refreshErr
}
