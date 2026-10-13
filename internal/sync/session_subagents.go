package sync

import (
	"context"
	"errors"
	"fmt"

	"go.kenn.io/agentsview/internal/parser"
)

var subagentTranscriptPaths = parser.ClaudeSubagentTranscriptPaths

// ErrSubagentRefreshIncomplete marks lock contention after the parent refresh.
var ErrSubagentRefreshIncomplete = errors.New(
	"subagent refresh incomplete; usage aggregate may be incomplete",
)

// SyncSessionWithSubagentsContext refreshes reachable archived and newly spawned sessions.
func (e *Engine) SyncSessionWithSubagentsContext(ctx context.Context, sessionID string) error {
	var refreshErr error
	seenIDs := make(map[string]bool)
	attemptedPaths := make(map[string]bool)
	queue := []string{sessionID}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return errors.Join(refreshErr, err)
		}
		id := queue[0]
		queue = queue[1:]
		if seenIDs[id] {
			continue
		}
		seenIDs[id] = true
		session, err := e.db.GetSession(ctx, id)
		if err != nil {
			return errors.Join(refreshErr, err)
		}
		path := e.FindSourceFile(ctx, id)
		// Retained intermediates still connect descendants while their source is absent.
		if !attemptedPaths[path] && (path != "" || session == nil || id == sessionID) {
			if path != "" {
				attemptedPaths[path] = true
			}
			err = e.SyncSingleSessionForUsageContext(ctx, id)
			refreshErr = errors.Join(refreshErr, err)
			if errors.Is(err, ErrSyncInProgress) {
				if id == sessionID {
					return refreshErr
				}
				return fmt.Errorf("%w: %w", ErrSubagentRefreshIncomplete, refreshErr)
			}
		}
		agent := parser.AgentClaude
		if def, ok := parser.AgentByPrefix(id); ok {
			agent = def.Type
		}
		if agent == parser.AgentClaude {
			if path == "" {
				path = e.db.GetSessionFilePath(ctx, id)
			}
			var paths []string
			for _, childPath := range subagentTranscriptPaths(path) {
				if !attemptedPaths[childPath] {
					attemptedPaths[childPath] = true
					paths = append(paths, childPath)
				}
			}
			if len(paths) > 0 {
				if isS3SourcePath(path) {
					err = e.SyncS3SubagentTranscriptsContext(ctx, id, agent, paths)
				} else {
					err = e.syncPathsContext(ctx, paths, false, false)
				}
				refreshErr = errors.Join(refreshErr, err)
				if errors.Is(err, ErrSyncInProgress) {
					return fmt.Errorf("%w: %w", ErrSubagentRefreshIncomplete, refreshErr)
				}
			}
		}
		children, err := e.db.GetChildSessions(ctx, id)
		if err != nil {
			return errors.Join(refreshErr, err)
		}
		for _, child := range children {
			queue = append(queue, child.ID)
		}
		if isCodexFormatAgent(agent) {
			spawned, err := e.db.SubagentChildSessionIDs(ctx, []string{id})
			if err != nil {
				return errors.Join(refreshErr, err)
			}
			queue = append(queue, spawned...)
		}
	}
	return refreshErr
}
