package worker

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/protocol"
)

func TestFrozenCommandAdmission(t *testing.T) {
	for _, scenario := range []string{"valid", "tampered", "wrong-command", "unsupported-contract"} {
		t.Run(scenario, func(t *testing.T) {
			p := protocol.Payload{Version: 1, Task: protocol.Task{ID: "t1", Description: json.RawMessage(`"$SECRET {{workspace}}"`)}, Repo: "owner/repo", Stage: "test", Mode: "test", Branch: "fix/t1", Base: "main", BaseSHA: "base", BranchSHA: "head", HeadSHA: "head", PR: 1, TimeoutSeconds: 60, Result: "test-verdict-v1", Instructions: "Work in {{workspace}}; write {{verdict_file}}", Comments: []protocol.Comment{}}
			if scenario == "unsupported-contract" {
				p.Result = "future-v9"
			}
			raw, _ := json.Marshal(p)
			command := protocol.Command{Version: 1, Task: "t1", Repo: p.Repo, Stage: p.Stage, Command: "cmd1", Prompt: protocol.Prompt{Template: p.Mode, Payload: string(raw), Digest: hash(string(raw))}}
			if scenario == "tampered" {
				command.Prompt.Payload += " "
			}
			if scenario == "wrong-command" {
				command.Command = "obsolete"
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Header.Get("Authorization") != "Bearer key" {
					t.Error("missing credential")
				}
				_ = json.NewEncoder(w).Encode(command)
			}))
			defer srv.Close()
			root := t.TempDir()
			r := runner{cfg: Config{Task: "t1", Command: "cmd1", Repo: p.Repo, Stage: p.Stage, DispatcherURL: srv.URL}, http: srv.Client(), key: "key", dir: filepath.Join(root, "repo"), archive: filepath.Join(root, "commands", "cmd1"), log: log.New(io.Discard, "", 0)}
			err := r.fetch(t.Context())
			if scenario != "valid" {
				if err == nil || r.payload.Version != 0 {
					t.Fatal("invalid command admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(r.prompt, "$SECRET {{workspace}}") || !strings.Contains(r.prompt, filepath.Join(r.archive, "verdict.txt")) {
				t.Fatal("literal context or local verdict path lost")
			}
			archived, err := os.ReadFile(filepath.Join(r.archive, "payload.json"))
			if err != nil || string(archived) != string(raw) {
				t.Fatal("exact snapshot not archived")
			}
			before := r.prompt
			if r.fetch(t.Context()) == nil {
				t.Fatal("archive reused")
			}
			after, _ := os.ReadFile(filepath.Join(r.archive, "prompt.txt"))
			if string(after) != before {
				t.Fatal("prior prompt overwritten")
			}
		})
	}
}

func TestReportsRequireExactAcknowledgment(t *testing.T) {
	for _, scenario := range []string{"accepted", "old-server", "different-command", "rejected"} {
		t.Run(scenario, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				var report protocol.Report
				if err := json.NewDecoder(req.Body).Decode(&report); err != nil {
					t.Error(err)
				}
				ack := protocol.Ack{Version: 1, Accepted: true, Task: report.Task, Command: report.Command, Event: report.Event}
				switch scenario {
				case "old-server":
					w.WriteHeader(202)
				case "different-command":
					ack.Command = "other"
				case "rejected":
					ack.Accepted = false
				}
				_ = json.NewEncoder(w).Encode(ack)
			}))
			defer srv.Close()
			r := runner{cfg: Config{DispatcherURL: srv.URL, Task: "t1", Command: "cmd1"}, http: srv.Client()}
			err := r.report(t.Context(), protocol.Report{Event: "author-started", PromptDigest: "hash"})
			if (err == nil) != (scenario == "accepted") {
				t.Fatalf("acknowledgment: %v", err)
			}
		})
	}
}
