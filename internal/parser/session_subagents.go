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
