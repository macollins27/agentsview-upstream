package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"go.kenn.io/agentsview/internal/parser"
)

var subagentTranscriptPaths = parser.ClaudeSubagentTranscriptPaths

// SyncSessionWithSubagentsContext refreshes one session, referenced children,
// and Claude subagent transcripts in its root tree. Child discovery still runs
// when the requested-session refresh fails so callers can retain archived data
// while ingesting any descendants that remain available.
func (e *Engine) SyncSessionWithSubagentsContext(
	ctx context.Context,
	sessionID string,
) error {
	return e.syncSessionWithSubagentsContext(ctx, sessionID, false, nil, nil)
}

// TrySyncSessionWithSubagentsContext tries archived batches and waits for first descendant imports.
func (e *Engine) TrySyncSessionWithSubagentsContext(ctx context.Context, sessionID string) error {
	return e.syncSessionWithSubagentsContext(ctx, sessionID, true, nil, nil)
}

func (e *Engine) syncSessionWithSubagentsContext(ctx context.Context, sessionID string, foreground bool, selected *parser.DiscoveredFile, publishedID *string) error {
	parentErr := e.syncSingleSessionContext(ctx, sessionID, foreground, selected, publishedID)
	if publishedID != nil {
		sessionID = *publishedID
	}
	if errors.Is(parentErr, ErrSyncInProgress) {
		return parentErr
	}
	referenceForeground := foreground

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
		return errors.Join(parentErr, e.syncReferencedSessionChildren(ctx, sessionID, referenceForeground))
	}
	if foreground {
		for _, path := range paths {
			ids, err := e.db.ListSessionIDsByFilePath(ctx, path, string(parentAgent))
			if err != nil {
				return errors.Join(parentErr, err)
			}
			if len(ids) == 0 {
				foreground = false
				break
			}
		}
	}

	var subagentErr error
	if strings.HasPrefix(sourcePath, "s3://") {
		subagentErr = e.syncS3SubagentTranscriptsContext(
			ctx, sessionID, parentAgent, paths, foreground)
	} else {
		subagentErr = e.syncPathsContext(ctx, paths, false, foreground)
	}
	return errors.Join(parentErr, subagentErr, e.syncReferencedSessionChildren(ctx, sessionID, referenceForeground))
}

func (e *Engine) syncReferencedSessionChildren(ctx context.Context, parentID string, foreground bool) error {
	visited := map[string]bool{parentID: true}
	pending := []string{parentID}
	var refreshErr error
	for len(pending) > 0 {
		id := pending[0]
		pending = pending[1:]
		children, err := e.db.SubagentChildSessionIDs(ctx, []string{id})
		if err != nil {
			return errors.Join(refreshErr, err)
		}
		for _, child := range children {
			if visited[child] {
				continue
			}
			visited[child] = true
			archived, err := e.db.GetSession(ctx, child)
			if err == nil {
				if archived == nil {
					var imported string
					imported, _, err = e.ImportSessionContext(ctx, child, false)
					if err == nil {
						child = imported
					}
				} else if def, ok := parser.AgentByPrefix(child); !ok || def.Type != parser.AgentCodebuff || e.db.GetSessionFilePath(ctx, id) != e.db.GetSessionFilePath(ctx, child) {
					err = e.syncSingleSessionContext(ctx, child, foreground, nil, nil)
				}
			}
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					err = fmt.Errorf("referenced child %s has no available source", child)
				}
				refreshErr = errors.Join(refreshErr, err)
				continue
			}
			visited[child] = true
			pending = append(pending, child)
		}
	}
	return refreshErr
}
