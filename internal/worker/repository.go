package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (r *runner) ensureRepository(ctx context.Context) (resultErr error) {
	info, err := os.Lstat(r.dir)
	if err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("repository path is not a directory")
		}
		origin, err := r.git(ctx, "config", "--get", "remote.origin.url")
		if err != nil {
			return err
		}
		if !repositoryOriginMatches(origin, r.cfg.Repo) {
			return fmt.Errorf("existing checkout belongs to a different repository; preserved")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(r.dir), ".kaneo-clone-")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tmp); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("clean up repository clone: %w", err))
		}
	}()
	clone := filepath.Join(tmp, "repo")
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if _, err := process(ctx, r.lock, tmp, "", toolEnv("", r.ghToken), "git",
		"-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential",
		"clone", "--quiet", "--", "https://github.com/"+r.cfg.Repo+".git", clone); err != nil {
		return err
	}
	if _, err := os.Lstat(r.dir); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("repository path appeared during clone; preserved")
	}
	return os.Rename(clone, r.dir)
}

func repositoryOriginMatches(origin, repo string) bool {
	for _, prefix := range []string{"https://github.com/", "git@github.com:", "ssh://git@github.com/"} {
		if slug, ok := strings.CutPrefix(origin, prefix); ok {
			return strings.EqualFold(strings.TrimSuffix(slug, ".git"), repo)
		}
	}
	return false
}
