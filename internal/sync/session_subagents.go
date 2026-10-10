package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"go.kenn.io/agentsview/internal/db"
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
	refreshedPaths := make(map[string]bool)
	if parentAgent == parser.AgentCodebuff {
		refreshedPaths[sourcePath] = parentErr == nil
	}
	if len(paths) == 0 {
		return errors.Join(parentErr, e.syncReferencedSessionChildren(ctx, sessionID, referenceForeground, refreshedPaths))
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
	if subagentErr == nil {
		for _, path := range paths {
			refreshedPaths[path] = true
		}
	}
	return errors.Join(parentErr, subagentErr, e.syncReferencedSessionChildren(ctx, sessionID, referenceForeground, refreshedPaths))
}

func (e *Engine) syncReferencedSessionChildren(ctx context.Context, parentID string, foreground bool, refreshedPaths map[string]bool) error {
	refreshed := map[string]bool{parentID: true}
	var refreshErr error
	_, walkErr := db.WalkDelegatedDescendants(ctx, parentID, false, func(ctx context.Context, id string) ([]db.Session, error) {
		if !refreshed[id] {
			refreshed[id] = true
			archived, err := e.db.GetSession(ctx, id)
			if err == nil {
				if archived == nil {
					var imported string
					imported, _, err = e.ImportSessionContext(ctx, id, false)
					if err == nil {
						id = imported
					}
				} else if !refreshedPaths[e.db.GetSessionFilePath(ctx, id)] {
					err = e.syncSingleSessionContext(ctx, id, foreground, nil, nil)
				}
			}
			if errors.Is(err, os.ErrNotExist) {
				err = fmt.Errorf("referenced child %s has no available source", id)
			}
			refreshErr = errors.Join(refreshErr, err)
			refreshed[id] = true
		}
		children, err := e.db.GetChildSessions(ctx, id)
		if err != nil {
			return nil, err
		}
		references, err := e.db.SubagentChildSessionIDs(ctx, []string{id})
		if err != nil {
			return nil, err
		}
		known := make(map[string]bool, len(children))
		for _, child := range children {
			known[child.ID] = true
		}
		for _, ref := range references {
			if !known[ref] {
				children = append(children, db.Session{ID: ref, RelationshipType: "subagent"})
			}
		}
		return children, nil
	})
	return errors.Join(refreshErr, walkErr)
}
