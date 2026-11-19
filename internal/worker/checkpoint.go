package worker

import (
	"context"
	"fmt"
	"strings"
)

// Refuse the entire publication before changing the index. Filtering a stash
// would silently omit work while claiming the branch is sufficient to recover.
// Include authored history: a later deletion must not hide a private file in
// an earlier commit that the push would also publish. This is a path policy,
// not content-based secret detection; repositories must still ignore secrets.
func (r *runner) checkCheckpointPaths(ctx context.Context) error {
	for _, args := range [][]string{
		{"diff", "--name-only", "--no-renames", "--diff-filter=AMT", "-z", "HEAD", "--"},
		{"ls-files", "--others", "--exclude-standard", "-z"},
		{"log", "--format=", "--name-only", "--no-renames", "--diff-filter=AMT", "--diff-merges=first-parent", "-z", r.payload.BaseSHA + "..HEAD", "--"},
	} {
		// Keep NUL-delimited paths verbatim, including leading whitespace.
		raw, err := r.tool(ctx, "git", args...)
		if err != nil {
			return fmt.Errorf("check checkpoint paths: %w", err)
		}
		for _, path := range strings.Split(raw, "\x00") {
			if privateCheckpointPath(path) {
				return fmt.Errorf("checkpoint held: protected path %q; all work retained locally; remove private material from the index and unpublished history, and ignore local runtime files before retrying", path)
			}
		}
	}
	return nil
}

func privateCheckpointPath(path string) bool {
	parts := strings.Split(strings.ToLower(path), "/")
	for i, part := range parts {
		switch part {
		case ".aws", ".ssh", ".kube", ".gnupg", ".cache", "__pycache__", "node_modules", ".venv", ".terraform", ".nightly", "books", "downloads", "references":
			return true
		case ".claude":
			if i+1 < len(parts) {
				switch parts[i+1] {
				case "projects", "debug", "todos", "session-env", "shell-snapshots":
					return true
				}
			}
		}
	}
	name := parts[len(parts)-1]
	switch name {
	case ".env", ".envrc", ".netrc", ".npmrc", ".pypirc", ".credentials.json", "credentials.json", "id_rsa", "id_ed25519", "id_ecdsa":
		return true
	}
	if strings.HasPrefix(name, ".env.") && name != ".env.example" && name != ".env.sample" && name != ".env.template" {
		return true
	}
	for _, suffix := range []string{".pem", ".key", ".p12", ".pfx", ".tfstate", ".tfstate.backup"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}
