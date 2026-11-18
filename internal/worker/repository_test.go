package worker

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/protocol"
)

func TestMissingRepositoryIsClonedAndPrepared(t *testing.T) {
	root := t.TempDir()
	for key, value := range map[string]string{
		"GIT_CONFIG_GLOBAL": filepath.Join(root, "gitconfig"), "GIT_CONFIG_NOSYSTEM": "1",
		"GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.invalid",
		"GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "test@example.invalid",
	} {
		t.Setenv(key, value)
	}
	git := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	remote := filepath.Join(root, "remote")
	git(root, "init", "-q", "--bare", "--initial-branch=main", remote)
	seed := filepath.Join(root, "seed")
	git(root, "clone", "-q", remote, seed)
	git(seed, "commit", "-qm", "initial", "--allow-empty")
	git(seed, "push", "-q", "origin", "main")
	sha := git(seed, "rev-parse", "HEAD")
	git(root, "config", "--global", "url."+remote+".insteadOf", "https://github.com/Owner/new-repo.git")
	r := runner{cfg: Config{Repo: "Owner/new-repo"}, dir: filepath.Join(root, "new-repo"), payload: protocol.Payload{Mode: "author", Base: "main", BaseSHA: sha, Branch: "fix/task"}}
	if err := r.ensureRepository(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.checkout(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := git(r.dir, "branch", "--show-current"); got != "fix/task" {
		t.Fatalf("branch=%s", got)
	}
	if err := r.ensureRepository(t.Context()); err != nil {
		t.Fatalf("existing assigned clone rejected: %v", err)
	}
	git(r.dir, "remote", "set-url", "origin", "https://github.com/Other/new-repo.git")
	if err := r.ensureRepository(t.Context()); err == nil {
		t.Fatal("reused another owner's checkout")
	}
	if got := git(r.dir, "config", "--get", "remote.origin.url"); got != "https://github.com/Other/new-repo.git" {
		t.Fatal("changed existing checkout")
	}
	r.dir = filepath.Join(root, "missing")
	git(root, "config", "--global", "url."+filepath.Join(root, "absent")+".insteadOf", "https://github.com/Owner/missing.git")
	r.cfg.Repo = "Owner/missing"
	if err := r.ensureRepository(t.Context()); err == nil {
		t.Fatal("missing remote accepted")
	}
	if _, err := os.Stat(r.dir); !os.IsNotExist(err) {
		t.Fatalf("failed clone left destination: %v", err)
	}
	if leftovers, err := filepath.Glob(filepath.Join(root, ".kaneo-clone-*")); err != nil || len(leftovers) != 0 {
		t.Fatalf("clone scratch left behind: %v %v", leftovers, err)
	}
}

func TestRepositoryOriginMustMatchAssignment(t *testing.T) {
	for origin, want := range map[string]bool{
		"https://github.com/Owner/repo.git":      true,
		"https://github.com/owner/REPO":          true,
		"git@github.com:Owner/repo.git":          true,
		"ssh://git@github.com/Owner/repo.git":    true,
		"https://github.com/Other/repo.git":      false,
		"https://github.com.evil/Owner/repo.git": false,
		"https://example.com/Owner/repo.git":     false,
	} {
		if got := repositoryOriginMatches(origin, "Owner/repo"); got != want {
			t.Errorf("repositoryOriginMatches(%q)=%v, want %v", origin, got, want)
		}
	}
}
