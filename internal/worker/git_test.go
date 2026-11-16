package worker

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/protocol"
)

func TestCheckoutPreservesWorkAndResumesPublishedBranches(t *testing.T) {
	for k, v := range map[string]string{"GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.invalid", "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "test@example.invalid"} {
		t.Setenv(k, v)
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
	root := t.TempDir()
	remote := filepath.Join(root, "remote")
	local := filepath.Join(root, "local")
	operator := filepath.Join(root, "operator")
	git(root, "init", "-q", "--bare", "--initial-branch=main", remote)
	git(root, "clone", "-q", remote, local)
	file := filepath.Join(local, "file")
	if err := os.WriteFile(file, []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	git(local, "add", "file")
	git(local, "commit", "-qm", "base")
	git(local, "push", "-qu", "origin", "main")
	git(local, "checkout", "-qb", "old-task")
	if err := os.WriteFile(file, []byte("task"), 0600); err != nil {
		t.Fatal(err)
	}
	git(local, "commit", "-qam", "task")
	git(local, "push", "-qu", "origin", "old-task")
	git(root, "clone", "-q", remote, operator)
	git(operator, "merge", "-q", "--squash", "origin/old-task")
	git(operator, "commit", "-qm", "squash")
	git(operator, "push", "-q", "origin", "main")
	git(operator, "push", "-q", "origin", "--delete", "old-task")
	base := git(operator, "rev-parse", "HEAD")
	r := runner{dir: local, payload: protocol.Payload{Mode: "author", Base: "main", BaseSHA: "stale", Branch: "fix/next"}}
	if r.checkout(t.Context()) == nil {
		t.Fatal("stale revision accepted")
	}
	r.payload.BaseSHA = base
	if err := r.checkout(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("unpublished"), 0600); err != nil {
		t.Fatal(err)
	}
	git(local, "commit", "-qam", "unpublished")
	before := git(local, "rev-parse", "HEAD")
	if r.checkout(t.Context()) == nil || git(local, "rev-parse", "HEAD") != before {
		t.Fatal("unpublished work lost")
	}
	git(operator, "checkout", "-qb", "fix/resume")
	git(operator, "push", "-qu", "origin", "fix/resume")
	fresh := filepath.Join(root, "fresh")
	git(root, "clone", "-q", remote, fresh)
	r.dir = fresh
	r.payload.Mode = "resume"
	r.payload.Branch = "fix/resume"
	r.payload.BranchSHA = base
	if err := r.checkout(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fresh, "file"), []byte("continued"), 0600); err != nil {
		t.Fatal(err)
	}
	git(fresh, "commit", "-qam", "continued")
	git(fresh, "-c", "push.autoSetupRemote=false", "-c", "push.default=simple", "push", "-q")
	if git(remote, "rev-parse", "fix/resume") != git(fresh, "rev-parse", "HEAD") {
		t.Fatal("resumed commit not published")
	}
}
