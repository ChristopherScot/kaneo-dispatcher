package worker

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/protocol"
)

func TestDirtyCheckoutPreservedAndReportedBlocked(t *testing.T) {
	for _, scenario := range []string{"note-accepted", "note-refused"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "repo")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			git := func(args ...string) string {
				t.Helper()
				cmd := exec.Command("git", args...)
				cmd.Dir = dir
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %v: %s", args, err, out)
				}
				return string(out)
			}
			git("init", "-q", "--initial-branch=main")
			git("remote", "add", "origin", "https://github.com/owner/repo.git")
			file := filepath.Join(dir, "unfinished.txt")
			if err := os.WriteFile(file, []byte("preserve this work\n"), 0600); err != nil {
				t.Fatal(err)
			}
			git("add", "unfinished.txt")
			before := git("status", "--porcelain")
			p := protocol.Payload{Version: protocol.Version, Task: protocol.Task{ID: "t1"}, Repo: "owner/repo", Stage: "author", Mode: "author", Branch: "fix/t1", Base: "main", BaseSHA: "base", TimeoutSeconds: 60, Result: "author-v1", Instructions: "Work in {{workspace}}", Comments: []protocol.Comment{}}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			command := protocol.Command{Version: protocol.Version, Task: "t1", Repo: p.Repo, Stage: p.Stage, Command: "cmd1", Prompt: protocol.Prompt{Template: p.Mode, Payload: string(raw), Digest: hash(string(raw))}}
			var mu sync.Mutex
			var reports []protocol.Report
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/worker-command" {
					_ = json.NewEncoder(w).Encode(command)
					return
				}
				var report protocol.Report
				if err := json.NewDecoder(req.Body).Decode(&report); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				mu.Lock()
				reports = append(reports, report)
				mu.Unlock()
				if scenario == "note-refused" && report.Event == "note" {
					w.WriteHeader(503)
					return
				}
				_ = json.NewEncoder(w).Encode(protocol.Ack{Version: protocol.Version, Accepted: true, Task: report.Task, Command: report.Command, Event: report.Event})
			}))
			defer srv.Close()
			r := runner{cfg: Config{Stage: "author", Task: "t1", Repo: p.Repo, Command: "cmd1", Owner: "owner", DispatcherURL: srv.URL}, http: srv.Client(), key: "key", dir: dir, archive: filepath.Join(root, "commands", "cmd1"), log: log.New(io.Discard, "", 0)}
			err = r.execute(t.Context())
			if err == nil || !strings.Contains(err.Error(), "local edits preserved") {
				t.Fatalf("lost checkout failure: %v", err)
			}
			if scenario == "note-refused" && !strings.Contains(err.Error(), "HTTP 503") {
				t.Fatalf("lost reporting failure: %v", err)
			}
			mu.Lock()
			got := slices.Clone(reports)
			mu.Unlock()
			if len(got) != 3 || got[0].Event != "author-started" || got[1].Event != "note" || got[2].Event != "author-blocked" || !strings.Contains(got[1].Message, "Worker setup failed") {
				t.Fatalf("incorrect lifecycle: %+v", got)
			}
			contents, err := os.ReadFile(file)
			if err != nil || string(contents) != "preserve this work\n" || git("status", "--porcelain") != before {
				t.Fatal("dirty checkout changed")
			}
		})
	}
}
