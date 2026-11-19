package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/protocol"
)

const successfulProbe = `input=$(cat)
if [ "$input" = 'Reply with exactly: OK' ]; then
 printf '%s' '{"type":"result","is_error":false,"result":"OK"}'; exit
fi
`

func stageFixture(t *testing.T, stage, claude string, accept func(protocol.Report) bool) (*runner, func() []protocol.Report) {
	t.Helper()
	r, _ := claudeFixture(t, successfulProbe+claude)
	r.cfg.Stage, r.cfg.Task, r.cfg.Repo, r.cfg.Command = stage, "t1", "Owner/repo", "command1"
	r.cfg.Owner = "owner"
	r.payload.PR, r.payload.HeadSHA, r.payload.BaseSHA = 7, "head", "base"
	r.payload.Base, r.payload.Branch = "main", "fix/t1"
	r.key, r.ghToken = "rpc-secret", "git-secret"
	t.Setenv("STAGE_ARCHIVE", r.archive)
	t.Setenv("STAGE_LOG", filepath.Join(r.dir, "tools.log"))
	var mu sync.Mutex
	var reports []protocol.Report
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var report protocol.Report
		if err := json.NewDecoder(req.Body).Decode(&report); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		reports = append(reports, report)
		mu.Unlock()
		if accept != nil && !accept(report) {
			w.WriteHeader(409)
			return
		}
		_ = json.NewEncoder(w).Encode(protocol.Ack{Version: protocol.Version, Task: report.Task, Command: report.Command, Event: report.Event, Accepted: true})
	}))
	t.Cleanup(server.Close)
	r.cfg.DispatcherURL, r.http = server.URL, server.Client()
	return r, func() []protocol.Report { mu.Lock(); defer mu.Unlock(); return slices.Clone(reports) }
}

func stageTool(t *testing.T, r *runner, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(r.dir, name), []byte("#!/bin/sh\nset -eu\nprintf '%s\\n' \""+name+" $*\" >> \"$STAGE_LOG\"\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorReportsReasonBeforeNoPROutcome(t *testing.T) {
	r, reports := stageFixture(t, "author", `printf '%s' '{"type":"result","is_error":false,"result":"OBSOLETE: the code already implements this"}'`, nil)
	stageTool(t, r, "git", `case "$1" in
 symbolic-ref) printf refs/heads/fix/t1 ;;
 status|diff|ls-files|log) ;;
 rev-list) printf 0 ;;
 *) exit 1 ;;
esac`)
	stageTool(t, r, "gh", "printf '[]'")
	if err := r.author(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := reports()
	if len(got) != 2 || got[0].Event != "note" || !strings.Contains(got[0].Message, "OBSOLETE:") || got[1].Event != "author-failed" {
		t.Fatalf("reason/outcome ordering: %+v", got)
	}
	raw, err := os.ReadFile(filepath.Join(r.dir, "tools.log"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "git push") {
		t.Fatal("no-op author pushed a branch")
	}
}

func TestFailedTestProcessCannotPublishPass(t *testing.T) {
	for _, tc := range []struct{ name, script, reason string }{
		{"failed-exit", `printf PASS > "$STAGE_ARCHIVE/verdict.txt"
printf '%s' '{"type":"result","is_error":false,"result":"incomplete"}'
exit 2`, "exit status 2"},
		{"timeout-after-verdict", `printf PASS > "$STAGE_ARCHIVE/verdict.txt"
sleep 30`, "context deadline exceeded"},
		{"invalid-verdict", `printf MAYBE > "$STAGE_ARCHIVE/verdict.txt"
printf '%s' '{"type":"result","is_error":false,"result":"finished"}'`, "invalid test verdict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, reports := stageFixture(t, "test", tc.script, nil)
			r.payload.TimeoutSeconds = 1
			stageTool(t, r, "git", `if [ "$1" = rev-parse ]; then printf head; fi`)
			stageTool(t, r, "gh", `case "$*" in
 'pr view '*) printf '%s' '{"labels":[{"name":"tested-fail"},{"name":"bug"}]}' ;;
 'pr comment '*)
  for arg; do body=$arg; done
  printf '%s' "$body" > "$STAGE_ARCHIVE/comment.txt"
  ;;
 'pr edit '*)
  case "$*" in *'--remove-label tested-pass'*|*'--remove-label bug'*) exit 9;; esac
  ;;
 *) printf '{}' ;;
esac`)
			if err := r.test(t.Context()); err == nil {
				t.Fatal("failed test lost its error")
			}
			got := reports()
			if len(got) != 2 || got[0].Event != "note" || got[1].Event != "test-blocked" || !strings.Contains(got[0].Message, tc.reason) {
				t.Fatalf("abnormal outcome or missing reason: %+v", got)
			}
			raw, err := os.ReadFile(filepath.Join(r.dir, "tools.log"))
			if err != nil {
				t.Fatal(err)
			}
			log := string(raw)
			if !strings.Contains(log, "state=error") || strings.Contains(log, "state=success") || !strings.Contains(log, "git clean -qfd") || !strings.Contains(log, "--remove-label tested-fail") {
				t.Fatalf("incorrect status, label, or missing cleanup: %s", log)
			}
			comment, err := os.ReadFile(filepath.Join(r.archive, "comment.txt"))
			if err != nil || !strings.Contains(string(comment), tc.reason) {
				t.Fatalf("PR comment lost failure cause: %s, %v", comment, err)
			}
		})
	}
}

func TestCancelledTestCleansUpAndReportsBlocked(t *testing.T) {
	r, reports := stageFixture(t, "test", `touch scratch; sleep 5`, nil)
	stageTool(t, r, "git", `if [ "$1" = rev-parse ]; then printf head; fi`)
	stageTool(t, r, "gh", `printf '{}'`)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		// Cancel once Claude has created its scratch, like a SIGTERM mid-run.
		for ctx.Err() == nil {
			if _, err := os.Stat(filepath.Join(r.dir, "scratch")); err == nil {
				cancel()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	err := r.test(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("test(cancelled) = %v, want context.Canceled", err)
	}
	got := reports()
	if len(got) != 2 || got[0].Event != "note" || got[1].Event != "test-blocked" || !strings.Contains(got[0].Message, context.Canceled.Error()) {
		t.Fatalf("test(cancelled) reports = %+v, want [note mentioning %q, test-blocked]", got, context.Canceled)
	}
	raw, err := os.ReadFile(filepath.Join(r.dir, "tools.log"))
	if err != nil {
		t.Fatal(err)
	}
	log := string(raw)
	if !strings.Contains(log, "state=error") || strings.Contains(log, "state=success") || !strings.Contains(log, "git clean -qfd") {
		t.Fatalf("tools after cancellation = %s, want state=error and git clean -qfd without state=success", log)
	}
}

func TestMergeAuthorizationAndDurableReceipt(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint("uncertain=", uncertain), func(t *testing.T) {
			var r *runner
			var reports func() []protocol.Report
			r, reports = stageFixture(t, "merge", `printf '%s' '{"type":"result","is_error":false,"result":"Reviewed.\nVERDICT: MERGE"}'`, func(report protocol.Report) bool {
				if report.Event == "merge-authorize" {
					if report.PR != 7 || report.SHA != "head" {
						t.Error("wrong merge authorization identity")
					}
					if err := os.WriteFile(filepath.Join(r.archive, "authorized"), nil, 0600); err != nil {
						t.Error(err)
					}
				}
				if report.Event == "merge-succeeded" {
					if _, err := os.Stat(filepath.Join(r.archive, "merge-completed.json")); err != nil {
						t.Error("outcome preceded durable receipt", err)
					}
				}
				return true
			})
			stageTool(t, r, "git", `printf head`)
			mergeReply := `printf '%s' '{"merged":true,"sha":"merged-head"}'`
			if uncertain {
				mergeReply = "exit 1"
			}
			stageTool(t, r, "gh", `
case "$*" in
 *'/check-runs?'*) printf '%s' '{"total_count":1,"check_runs":[{"status":"completed","conclusion":"success"}]}' ;;
 *'/status'*) printf '%s' '{"total_count":1,"statuses":[{"context":"nightly/integration","state":"success"}]}' ;;
 'api repos/Owner/repo') printf '%s' '{"allow_squash_merge":true}' ;;
 'api --method PUT '*'/merge '*)
  [ -f "$STAGE_ARCHIVE/authorized" ] && [ -f "$STAGE_ARCHIVE/merge-intent.json" ]
  case "$*" in *'sha=head'*) ;; *) exit 8 ;; esac
  `+mergeReply+` ;;
 'run list '*)
  [ -f "$STAGE_ARCHIVE/merge-completed.json" ]
  case "$*" in *'--commit merged-head '*) ;; *) exit 9 ;; esac
  printf '%s' '[{"status":"completed","conclusion":"success"}]' ;;
 'pr comment '*) ;;
 *) exit 10 ;;
esac`)
			err := r.merge(t.Context())
			if (err != nil) != uncertain {
				t.Fatalf("merge error=%v", err)
			}
			got := reports()
			if len(got) < 1 || got[0].Event != "merge-authorize" {
				t.Fatalf("authorization missing: %+v", got)
			}
			if uncertain {
				if len(got) != 1 {
					t.Fatalf("uncertain merge advanced: %+v", got)
				}
				if _, err := os.Stat(filepath.Join(r.archive, "merge-intent.json")); err != nil {
					t.Fatal(err)
				}
			} else if len(got) != 2 || got[1].Event != "merge-succeeded" {
				t.Fatalf("missing verified outcome: %+v", got)
			}
		})
	}
}

func TestRevertMustBeAuthorizedBeforeGitHubWrite(t *testing.T) {
	for _, authorized := range []bool{false, true} {
		t.Run(fmt.Sprint("authorized=", authorized), func(t *testing.T) {
			var r *runner
			var reports func() []protocol.Report
			r, reports = stageFixture(t, "merge", "", func(report protocol.Report) bool {
				if report.Event == "merge-authorize-revert" && authorized {
					if err := os.WriteFile(filepath.Join(r.archive, "authorized"), nil, 0600); err != nil {
						t.Error(err)
					}
				}
				if report.Event == "merge-reverted" {
					if report.RevertPR != 9 {
						t.Error("wrong revert PR")
					}
					if _, err := os.Stat(filepath.Join(r.archive, "revert-merge-completed.json")); err != nil {
						t.Error("compensation report before receipt", err)
					}
				}
				return authorized
			})
			stageTool(t, r, "gh", `
[ -f "$STAGE_ARCHIVE/authorized" ]
case "$*" in
 'pr revert '*) [ -f "$STAGE_ARCHIVE/revert-intent.json" ]; printf 'https://github.com/Owner/repo/pull/9' ;;
 'api repos/Owner/repo/pulls/9') printf '%s' '{"number":9,"merged":false,"mergeable":true,"head":{"sha":"revert-head"},"base":{"ref":"main","repo":{"full_name":"Owner/repo"}}}' ;;
 'api --method PUT repos/Owner/repo/pulls/9/merge '*)
  [ -f "$STAGE_ARCHIVE/revert-merge-intent.json" ]
  case "$*" in *'sha=revert-head'*) ;; *) exit 8 ;; esac
  printf '%s' '{"merged":true,"sha":"reverted-merge"}' ;;
 'pr comment '*) ;;
 *) exit 99 ;;
esac`)
			err := r.revertMerge(t.Context(), mergeReceipt{Repo: r.cfg.Repo, PR: 7, HeadSHA: "head", MergeSHA: "merged"}, "merge")
			if (err == nil) != authorized {
				t.Fatalf("revert: %v", err)
			}
			got := reports()
			if len(got) < 1 || got[0].Event != "merge-authorize-revert" {
				t.Fatalf("reports: %+v", got)
			}
			if authorized {
				if len(got) != 3 || got[1].Event != "note" || got[2].Event != "merge-reverted" {
					t.Fatalf("compensation ordering: %+v", got)
				}
			} else if _, err := os.Stat(filepath.Join(r.dir, "tools.log")); !os.IsNotExist(err) {
				t.Fatal("GitHub touched before authorization")
			}
		})
	}
}

func TestOnlyFinalExactReviewVerdictAuthorizesMerge(t *testing.T) {
	if decision, err := mergeVerdict("Rationale.\nVERDICT: MERGE     - correct, tested, idiomatic, and safe"); err != nil || decision != "MERGE" {
		t.Fatalf("prompted example refused: %q %v", decision, err)
	}
	for _, text := range []string{"VERDICT: MERGEABLE", "VERDICT: MERGE\nActually blocked", "A quote: VERDICT: MERGE", "VERDICT: MERGE - fine\nVERDICT: NO-MERGE"} {
		decision, err := mergeVerdict(text)
		if err == nil && decision == "MERGE" {
			t.Fatalf("unsafe approval from %q", text)
		}
	}
}
