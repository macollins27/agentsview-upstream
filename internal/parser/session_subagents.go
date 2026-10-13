package parser

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
)

// SubagentTranscriptRoot returns the directory or prefix holding a session's child sources.
func SubagentTranscriptRoot(agent AgentType, sessionPath string) string {
	if sessionPath == "" {
		return ""
	}
	switch agent {
	case AgentCodex, AgentTraeX, AgentAugureCode:
		return ""
	case AgentShelley:
		if _, _, ok := parseShelleyVirtualPath(sessionPath); ok {
			return sessionPath
		}
		return ""
	case AgentPositAssistant:
		return filepath.Join(filepath.Dir(sessionPath), positAssistantSubagentsDir)
	case AgentPi, AgentOMP, AgentOMO, AgentStepCode:
		if root, ok := strings.CutSuffix(sessionPath, ".jsonl"); ok {
			return root
		}
		return ""
	case AgentCursor:
		if isS3URI(sessionPath) {
			sourceRoot := CursorS3SourceRoot(sessionPath)
			rel := strings.TrimPrefix(sessionPath, sourceRoot+"/")
			loc, ok := cursorS3TranscriptLocation(strings.Split(rel, "/"))
			if !ok {
				return ""
			}
			return sourceRoot + "/" + loc.ProjectDir + "/agent-transcripts/" + loc.RawID + "/" + cursorSubagentsDirName
		}
		loc, ok := cursorTranscriptLocationFromPath(sessionPath)
		if !ok {
			return ""
		}
		dir := filepath.Dir(sessionPath)
		if loc.ParentRawID != "" {
			dir = filepath.Dir(filepath.Dir(dir))
		} else if filepath.Base(dir) == loc.RawID {
			dir = filepath.Dir(dir)
		}
		return filepath.Join(dir, loc.RawID, cursorSubagentsDirName)
	default:
		return ClaudeSubagentTranscriptRoot(sessionPath)
	}
}

// SubagentTranscriptPaths discovers children within the requested session's source tree.
func SubagentTranscriptPaths(ctx context.Context, agent AgentType, sessionPath string) ([]string, error) {
	root := SubagentTranscriptRoot(agent, sessionPath)
	if root == "" {
		return nil, nil
	}
	switch agent {
	case AgentShelley:
		dbPath, parentID, _ := parseShelleyVirtualPath(sessionPath)
		conn, err := OpenShelleyDB(dbPath)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		rows, err := conn.QueryContext(ctx,
			`WITH RECURSIVE parents AS MATERIALIZED (
				SELECT conversation_id AS id, TRIM(parent_conversation_id) AS parent FROM conversations
			), descendants(id) AS (
				SELECT id FROM parents WHERE parent = ?
				UNION
				SELECT c.id FROM parents c JOIN descendants d ON c.parent = d.id
			)
			SELECT id FROM descendants ORDER BY id`,
			parentID,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var paths []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			if IsValidSessionID(id) {
				paths = append(paths, ShelleyVirtualPath(dbPath, id))
			}
		}
		return paths, rows.Err()
	case AgentPositAssistant:
		var sources []SourceRef
		positAssistantSourceSet{}.collectConversationSources("", filepath.Dir(sessionPath), "", "", &sources, make(map[string]struct{}))
		paths := make([]string, 0, len(sources))
		for _, source := range sources {
			if source.DisplayPath != sessionPath {
				paths = append(paths, source.DisplayPath)
			}
		}
		slices.Sort(paths)
		return paths, nil
	case AgentPi, AgentOMP, AgentOMO, AgentStepCode:
		sources, err := NewJSONLSourceSet(agent, []string{root}, WithRecursive(), WithSymlinkFollowing(), WithIncludePath(isPiSourcePath)).Discover(ctx)
		paths := make([]string, 0, len(sources))
		for _, source := range sources {
			paths = append(paths, source.DisplayPath)
		}
		return paths, err
	case AgentCursor:
		if isS3URI(sessionPath) {
			return cursorS3SubagentTranscriptPaths(ctx, sessionPath)
		}
		seen := make(map[string]string)
		cursorCollectSubagentTranscripts(seen, root)
		paths := make([]string, 0, len(seen))
		for _, path := range seen {
			paths = append(paths, path)
		}
		slices.Sort(paths)
		return paths, nil
	default:
		return ClaudeSubagentTranscriptPaths(sessionPath), nil
	}
}
