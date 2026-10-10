package db

import "context"

// WalkDelegatedDescendants shares the relationship rules used by usage and refresh.
func WalkDelegatedDescendants(
	ctx context.Context, rootID string, rootDelegated bool, childrenFor func(context.Context, string) ([]Session, error),
) ([]Session, error) {
	type walkState struct {
		id        string
		delegated bool
	}
	visited := map[walkState]struct{}{
		{id: rootID, delegated: rootDelegated}: {},
	}
	included := make(map[string]struct{})
	queue := []walkState{{id: rootID, delegated: rootDelegated}}
	var out []Session
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		parent := queue[0]
		queue = queue[1:]
		children, err := childrenFor(ctx, parent.id)
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if child.ID == rootID {
				continue
			}
			childDelegated := parent.delegated ||
				child.RelationshipType == "subagent"
			include := child.RelationshipType == "subagent" || (parent.delegated && child.RelationshipType == "fork")
			if include {
				if _, ok := included[child.ID]; !ok {
					included[child.ID] = struct{}{}
					out = append(out, child)
				}
			}
			next := walkState{id: child.ID, delegated: childDelegated}
			if _, ok := visited[next]; ok {
				continue
			}
			visited[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	return out, nil
}
