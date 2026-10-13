package sync

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.kenn.io/agentsview/internal/parser"
)

var (
	subagentTranscriptPaths       = parser.SubagentTranscriptPaths
	discoverCodexS3SessionURIs    = parser.DiscoverCodexS3SessionURIs
	discoverCursorS3SubagentPaths = parser.DiscoverCursorS3SubagentPaths
)

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
	discovery := subagentSourceDiscovery{engine: e, codexS3: make(map[string]map[string]string), cursorS3: make(map[string]map[string][]string)}
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
			path, err = discovery.codexSpawnPath(ctx, id, parentID)
			refreshErr = errors.Join(refreshErr, err)
			if path != "" {
				s3ParentID = parentID
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
		if root := discovery.transcriptRoot(agent, path); root != "" && !discoveredRoots[root] {
			discoveredRoots[root] = true
			var paths []string
			discovered, err := discovery.transcriptPaths(ctx, agent, path, root)
			refreshErr = errors.Join(refreshErr, err)
			for _, childPath := range discovered {
				if childRoot := discovery.transcriptRoot(agent, childPath); childRoot != "" && (parser.SubagentPathsIncludeDescendants(agent, path) || samePathOrDescendant(childRoot, root)) {
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

type subagentSourceDiscovery struct {
	engine   *Engine
	codexS3  map[string]map[string]string
	cursorS3 map[string]map[string][]string
}

func (d *subagentSourceDiscovery) codexSpawnPath(ctx context.Context, id, parentID string) (string, error) {
	parentPath := d.engine.db.GetSessionFilePath(ctx, parentID)
	host, rawID := parser.StripHostPrefix(id)
	parentHost, _ := parser.StripHostPrefix(parentID)
	if !isS3SourcePath(parentPath) || host != parentHost || !strings.HasPrefix(rawID, "codex:") {
		return "", nil
	}
	configuredRoot := ""
	for _, root := range d.engine.sources().agentDirs[parser.AgentCodex] {
		if _, ok := s3RelFromRoot(root, parentPath); ok {
			configuredRoot = root
			break
		}
	}
	root, ok := parser.CodexS3SourceRoot(configuredRoot, parentPath)
	if !ok {
		return "", nil
	}
	uris, listed := d.codexS3[root]
	var err error
	if !listed {
		uris, err = discoverCodexS3SessionURIs(root)
		d.codexS3[root] = uris
	}
	return uris[strings.TrimPrefix(rawID, "codex:")], err
}

func (d *subagentSourceDiscovery) cursorSourceRoot(path string) string {
	return parser.CursorS3SourceRoot(path, d.engine.sources().agentDirs[parser.AgentCursor]...)
}

func (d *subagentSourceDiscovery) transcriptRoot(agent parser.AgentType, path string) string {
	if agent == parser.AgentCursor && isS3SourcePath(path) {
		return parser.CursorS3SubagentRoot(d.cursorSourceRoot(path), path)
	}
	return parser.SubagentTranscriptRoot(agent, path)
}

func (d *subagentSourceDiscovery) transcriptPaths(ctx context.Context, agent parser.AgentType, path, root string) ([]string, error) {
	if agent != parser.AgentCursor || !isS3SourcePath(path) {
		return subagentTranscriptPaths(ctx, agent, path)
	}
	sourceRoot := d.cursorSourceRoot(path)
	inventory, listed := d.cursorS3[sourceRoot]
	var err error
	if !listed {
		inventory, err = discoverCursorS3SubagentPaths(ctx, sourceRoot)
		d.cursorS3[sourceRoot] = inventory
	}
	return inventory[root], err
}
