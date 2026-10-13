package sync

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.kenn.io/agentsview/internal/parser"
)

var subagentTranscriptPaths = parser.SubagentTranscriptPaths
var discoverCodexS3SessionURIs = parser.DiscoverCodexS3SessionURIs

// ErrSubagentRefreshIncomplete marks lock contention after the parent refresh.
var ErrSubagentRefreshIncomplete = errors.New(
	"subagent refresh incomplete; usage aggregate may be incomplete",
)

// SyncSessionWithSubagentsContext refreshes reachable archived and newly spawned sessions.
func (e *Engine) SyncSessionWithSubagentsContext(ctx context.Context, sessionID string) error {
	var refreshErr error
	seenIDs := make(map[string]bool)
	attemptedPaths := make(map[string]bool)
	discoveredRoots := make(map[string]bool)
	codexS3Sources := make(map[string]map[string]string)
	queue := []string{sessionID}
	sourceParents := make(map[string]string)
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
		if session == nil && id != sessionID && (e.db.IsSessionExcluded(ctx, id) || e.db.IsSessionTrashed(ctx, id)) {
			continue
		}
		path := e.FindSourceFile(ctx, id)
		s3ParentID := ""
		if path == "" && session == nil {
			parentID := sourceParents[id]
			parentPath := e.db.GetSessionFilePath(ctx, parentID)
			host, rawID := parser.StripHostPrefix(id)
			parentHost, _ := parser.StripHostPrefix(parentID)
			if isS3SourcePath(parentPath) && host == parentHost && strings.HasPrefix(rawID, "codex:") {
				configuredRoot := ""
				for _, root := range e.sources().agentDirs[parser.AgentCodex] {
					if _, ok := s3RelFromRoot(root, parentPath); ok {
						configuredRoot = root
						break
					}
				}
				if root, ok := parser.CodexS3SourceRoot(configuredRoot, parentPath); ok {
					uris, listed := codexS3Sources[root]
					if !listed {
						uris, err = discoverCodexS3SessionURIs(root)
						codexS3Sources[root] = uris
						refreshErr = errors.Join(refreshErr, err)
					}
					if uri := uris[strings.TrimPrefix(rawID, "codex:")]; uri != "" {
						path, s3ParentID = uri, parentID
					}
				}
			}
		}
		// Retained intermediates still connect descendants while their source is absent.
		if !attemptedPaths[path] && (path != "" || session == nil || id == sessionID) {
			if path != "" {
				attemptedPaths[path] = true
			}
			if s3ParentID != "" {
				err = e.SyncS3SubagentTranscriptsContext(ctx, s3ParentID, parser.AgentCodex, []string{path})
			} else {
				err = e.SyncSingleSessionForUsageContext(ctx, id)
			}
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
		if path == "" {
			path = e.db.GetSessionFilePath(ctx, id)
		}
		if root := parser.SubagentTranscriptRoot(agent, path); root != "" && !discoveredRoots[root] {
			discoveredRoots[root] = true
			var paths []string
			discovered, err := subagentTranscriptPaths(ctx, agent, path)
			refreshErr = errors.Join(refreshErr, err)
			for _, childPath := range discovered {
				if childRoot := parser.SubagentTranscriptRoot(agent, childPath); childRoot != "" && samePathOrDescendant(childRoot, root) {
					discoveredRoots[childRoot] = true
				}
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
		spawned, err := e.db.SubagentChildSessionIDs(ctx, []string{id})
		if err != nil {
			return errors.Join(refreshErr, err)
		}
		for _, childID := range spawned {
			if sourceParents[childID] == "" {
				sourceParents[childID] = id
			}
			queue = append(queue, childID)
		}
	}
	return refreshErr
}
