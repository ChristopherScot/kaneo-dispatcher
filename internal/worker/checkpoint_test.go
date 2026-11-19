package worker

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthorCheckpointsUnfinishedWork(t *testing.T) {
	for _, mode := range []string{"unfinished", "cancelled", "clean-cancelled", "push-rejected", "wrong-branch", "private-untracked", "private-staged", "private-history"} {
		t.Run(mode, func(t *testing.T) {
			r, reports := stageFixture(t, "author", `printf '%s' '{"type":"result","is_error":false,"result":"unfinished"}'`, nil)
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
			write := func(path, value string) {
				t.Helper()
				if err := os.WriteFile(path, []byte(value), 0700); err != nil {
					t.Fatal(err)
				}
			}
			root := t.TempDir()
			remote := filepath.Join(root, "remote")
			local := filepath.Join(root, "local")
			git(root, "init", "-q", "--bare", "--initial-branch=main", remote)
			git(root, "clone", "-q", remote, local)
			write(filepath.Join(local, "tracked"), "base")
			write(filepath.Join(local, ".gitignore"), "ignored\n.env.ignored\n")
			git(local, "add", ".")
			git(local, "commit", "-qm", "base")
			git(local, "push", "-q", "origin", "main")
			r.dir = local
			r.payload.BaseSHA = git(local, "rev-parse", "HEAD")
			git(local, "checkout", "-qb", r.payload.Branch)
			git(local, "push", "-qu", "origin", r.payload.Branch)
			if mode == "wrong-branch" {
				git(local, "checkout", "-qb", "unrelated")
			}
			if strings.HasPrefix(mode, "private-") {
				write(filepath.Join(local, ".env.local"), "DUMMY_TEST_SECRET=do-not-push\n")
				if mode != "private-untracked" {
					git(local, "add", ".env.local")
				}
				if mode == "private-history" {
					git(local, "commit", "-qm", "private file")
					git(local, "rm", "-q", ".env.local")
					git(local, "commit", "-qm", "remove private file")
				}
			}
			if mode != "clean-cancelled" {
				write(filepath.Join(local, "staged"), "staged work")
				git(local, "add", "staged")
				write(filepath.Join(local, "tracked"), "unstaged work")
				write(filepath.Join(local, "untracked"), "new work")
				write(filepath.Join(local, "ignored"), "ignored scratch")
				write(filepath.Join(local, ".env.ignored"), "ignored private material")
			}
			if mode == "push-rejected" {
				write(filepath.Join(remote, "hooks", "pre-receive"), "#!/bin/sh\nexit 1\n")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode != "unfinished" {
				cancel()
			}
			beforeHead := git(local, "rev-parse", "HEAD")
			beforeIndex := git(local, "write-tree")
			beforeStatus := git(local, "status", "--porcelain")
			err := r.author(ctx)
			if mode != "unfinished" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
			if mode == "unfinished" && err != nil {
				t.Fatal(err)
			}
			got := reports()
			if len(got) != 2 || got[0].Event != "note" || got[1].Event != "author-blocked" {
				t.Fatalf("unfinished work advanced: %+v", got)
			}
			localHead := git(local, "rev-parse", "HEAD")
			remoteHead := git(remote, "rev-parse", r.payload.Branch)
			if strings.HasPrefix(mode, "private-") {
				if localHead != beforeHead || remoteHead != r.payload.BaseSHA || git(local, "write-tree") != beforeIndex || git(local, "status", "--porcelain") != beforeStatus {
					t.Fatal("refused checkpoint changed local work or published private material")
				}
				if !strings.Contains(got[0].Message, "protected path") || !strings.Contains(got[0].Message, ".env.local") {
					t.Fatalf("checkpoint refusal missing reason: %+v", got)
				}
				return
			}
			if mode == "clean-cancelled" {
				if localHead != r.payload.BaseSHA || remoteHead != r.payload.BaseSHA || !strings.Contains(got[0].Message, "No commits were made on "+r.payload.Branch) {
					t.Fatalf("clean cancelled run: heads %s/%s, note %q; want base and no commits on %s", localHead, remoteHead, got[0].Message, r.payload.Branch)
				}
				return
			}
			if mode == "wrong-branch" {
				if localHead != r.payload.BaseSHA || remoteHead != r.payload.BaseSHA || git(local, "status", "--porcelain") == "" {
					t.Fatal("unrelated checkout modified")
				}
				return
			}
			if !strings.HasPrefix(git(local, "log", "-1", "--format=%s"), "stash:") {
				t.Fatal("missing checkpoint commit")
			}
			if git(local, "status", "--porcelain") != "" {
				t.Fatal("work left uncommitted")
			}
			for file, want := range map[string]string{"staged": "staged work", "tracked": "unstaged work", "untracked": "new work"} {
				if got := git(local, "show", "HEAD:"+file); got != want {
					t.Fatalf("%s: %q", file, got)
				}
			}
			if strings.Contains(git(local, "ls-tree", "--name-only", "HEAD"), "ignored") {
				t.Fatal("ignored file committed")
			}
			if mode == "push-rejected" {
				if remoteHead != r.payload.BaseSHA || localHead == remoteHead || !strings.Contains(got[0].Message, "Could not publish") {
					t.Fatal("failed push lost local checkpoint or claimed success")
				}
				return
			}
			if remoteHead != localHead {
				t.Fatal("checkpoint not pushed")
			}
			if !strings.Contains(got[0].Message, r.payload.Branch+" at "+remoteHead) {
				t.Fatalf("note = %q, want it to mention %q", got[0].Message, r.payload.Branch+" at "+remoteHead)
			}
		})
	}
}
