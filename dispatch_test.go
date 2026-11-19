package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/worker"
)

func TestUnsignedPostIsRefused(t *testing.T) {
	f := newFixture(t)
	if w := f.post("/", move("t1", "to-do")); w.Code != http.StatusUnauthorized {
		t.Errorf("unsigned POST / = %d, want 401", w.Code)
	}
	f.wantNoLaunch()
}

func TestWrongSignatureIsRefused(t *testing.T) {
	f := newFixture(t)
	body := move("t1", "to-do")
	for _, tc := range []struct{ name, sig string }{
		{"OtherSecret", sign(body + "x")},
		{"NotHex", "zz"},
		{"Short", "abcd"},
	} {
		if w := f.post("/", body, "X-Kaneo-Signature", tc.sig); w.Code != http.StatusUnauthorized {
			t.Errorf("POST / with %s signature = %d, want 401", tc.name, w.Code)
		}
	}
	f.wantNoLaunch()
}

// Node handed a repeated header over as an array, and Buffer.from(array)
// threw - a valid delivery answered 500. Go's Header.Get takes the first
// value, so the case cannot recur; this pins that.
func TestDuplicateSignatureHeaderStillVerifies(t *testing.T) {
	f := newFixture(t)
	body := move("t1", "backlog")
	w := f.post("/", body, "X-Kaneo-Signature", sign(body), "X-Kaneo-Signature", sign(body))
	if w.Code != http.StatusAccepted {
		t.Errorf("duplicated header = %d, want 202", w.Code)
	}
}

func TestBodyOverOneMiBIsRefused(t *testing.T) {
	f := newFixture(t)
	body := strings.Repeat("x", maxBody+1)
	if w := f.signed("/", body); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body = %d, want 413", w.Code)
	}
	// Exactly the cap is still read and, being unparseable, still 202.
	body = strings.Repeat("x", maxBody)
	if w := f.signed("/", body); w.Code != http.StatusAccepted {
		t.Errorf("body at the cap = %d, want 202", w.Code)
	}
}

func TestInertColumnIsLoggedNotDispatched(t *testing.T) {
	f := newFixture(t)
	if w := f.signed("/", `{"event":"task.status_changed","task":{"id":"t1","ticketId":"T-1"},"data":{"newStatus":"in-progress"}}`); w.Code != http.StatusAccepted {
		t.Fatalf("inert column = %d, want 202", w.Code)
	}
	f.wantLog(`msg=noop task=T-1 column=in-progress`)
	f.wantUntouched()
	if n := f.kaneo.calls(); n != 0 {
		t.Errorf("an inert column looked up labels (%d Kaneo calls)", n)
	}
}

func TestOtherEventsAreIgnored(t *testing.T) {
	f := newFixture(t)
	f.signed("/", `{"event":"task.created","task":{"id":"t1"},"data":{"newStatus":"to-do"}}`)
	f.wantLog("msg=ignore event=task.created")
	f.wantNoLaunch()
}

func TestUnparseableBodyIsAcceptedAndLogged(t *testing.T) {
	f := newFixture(t)
	if w := f.signed("/", "not json"); w.Code != http.StatusAccepted {
		t.Errorf("unparseable body = %d, want 202 (the signature was fine)", w.Code)
	}
	f.wantLog(`reason="unparseable body"`)
}

func TestDispatchingColumnLaunchesTheStageInItsPod(t *testing.T) {
	f := newFixture(t)
	f.kaneo.labels["t2"] = []string{"bug", "repo:testrepo"}
	if w := f.signed("/", `{"event":"task.status_changed","task":{"id":"t2","ticketId":"CLA-22"},"data":{"newStatus":"to-do"}}`); w.Code != http.StatusAccepted {
		t.Fatalf("got %d, want 202", w.Code)
	}
	l := f.pods.launches()
	if len(l) != 1 {
		t.Fatalf("launches = %v, want exactly one", l)
	}
	if l[0].pod != "claude-session-0" {
		t.Errorf("launched in %s, want claude-session-0 for to-do", l[0].pod)
	}
	// The script, the raw task id and the resolved repo arrive as
	// positional arguments, so none is spliced into shell text. The repo
	// is there because stage-author refuses a named task without one.
	want := []string{"sh", "-c", launchLine, "/usr/local/bin/stage-author", "t2", "TestOwner/testrepo"}
	if len(l[0].argv) != len(want)+1 || !slices.Equal(l[0].argv[:len(want)], want) || l[0].argv[len(want)] == "" {
		t.Errorf("argv = %q, want %q", l[0].argv, want)
	}
	f.wantLog("msg=start task=CLA-22 repo=TestOwner/testrepo pod=claude-session-0 script=stage-author")
	f.wantLog("msg=launched task=CLA-22 repo=TestOwner/testrepo rc=0")
}

// The set sleeps at 0 replicas when idle, so the busy probe would fail
// against a pod that does not exist and read as busy forever. The wake
// comes first, once, and only when there is something to launch.
func TestSessionPodsAreWokenBeforeTheBusyProbe(t *testing.T) {
	f := newFixture(t)
	f.kaneo.labels["t2"] = []string{"repo:testrepo"}
	f.signed("/", move("t2", "to-do"))
	want := []string{"wake", "exec", "exec"} // the wake, the probe, the launch
	if got := f.pods.actions(); !slices.Equal(got, want) {
		t.Errorf("actions = %q, want %q", got, want)
	}
	if len(f.pods.launches()) != 1 {
		t.Error("the stage was not launched after the wake")
	}
}

// A wake that fails must be reported as such, not as "cannot read
// labels" or as a busy pod, and nothing may be exec'd into a set that
// never came up.
func TestFailedWakeIsLoggedAndNothingIsExecd(t *testing.T) {
	f := newFixture(t)
	f.pods.wakeErr = errors.New("claude-session not ready after 10m0s of waking (1 of 3 ready)")
	f.kaneo.labels["t2"] = []string{"repo:testrepo"}
	f.signed("/", move("t2", "to-do"))
	if calls := f.pods.all(); len(calls) != 0 {
		t.Errorf("exec'd into a set that did not wake: %v", calls)
	}
	f.wantLog(`card="t2: wake: claude-session not ready after 10m0s of waking (1 of 3 ready)"`)
}

// Stages are pinned to pods by ordinal, so the wake must cover the
// highest ordinal any route names, and it is derived from routes so a
// fourth stage cannot be added without being woken.
func TestWakeCoversEveryRoutedPod(t *testing.T) {
	if got := wakeReplicas(); got != 3 {
		t.Errorf("wakeReplicas() = %d, want 3 for claude-session-0..2", got)
	}
	if got := (stage{ordinal: 2}).pod(); got != "claude-session-2" {
		t.Errorf("stage 2 runs in %q, want claude-session-2", got)
	}
}

func TestEachColumnRoutesToItsOwnPod(t *testing.T) {
	for _, tc := range []struct {
		column, pod, script string
	}{
		{"to-do", "claude-session-0", "/usr/local/bin/stage-author"},
		{"in-review", "claude-session-2", "/usr/local/bin/stage-test"},
		{"mergeable", "claude-session-1", "/usr/local/bin/stage-merge"},
	} {
		t.Run(tc.column, func(t *testing.T) {
			f := newFixture(t)
			f.kaneo.labels["t"] = []string{"repo:testrepo"}
			if tc.column != "to-do" {
				pr := openPR("abc")[0]
				pr["head"].(map[string]any)["ref"] = "fix/t"
				f.github.prs["TestOwner:fix/t"] = []map[string]any{pr}
			}
			f.signed("/", move("t", tc.column))
			l := f.pods.launches()
			if len(l) != 1 || l[0].pod != tc.pod || l[0].argv[3] != tc.script {
				t.Errorf("%s launched %v, want %s running %s", tc.column, l, tc.pod, tc.script)
			}
		})
	}
}

func TestBusyProbeRunsInTheStagePodBeforeTheLaunch(t *testing.T) {
	f := newFixture(t)
	f.kaneo.labels["t2"] = []string{"repo:testrepo"}
	f.signed("/", move("t2", "to-do"))
	calls := f.pods.all()
	if len(calls) != 2 {
		t.Fatalf("calls = %v, want the busy probe then the launch", calls)
	}
	want := []string{"sh", "-c", busyProbe}
	if calls[0].pod != "claude-session-0" || !slices.Equal(calls[0].argv, want) {
		t.Errorf("first call = %v, want %q in claude-session-0", calls[0], want)
	}
}

// The remote probe and the Go worker must share the lock, override included.
func TestBusyProbeLocksWhatTheWorkerLocks(t *testing.T) {
	lockExpr := `"${NIGHTLY_LOCK:-` + worker.DefaultLock + `}"`
	if !strings.Contains(busyProbe, lockExpr) {
		t.Fatalf("busyProbe = %q does not lock %s", busyProbe, lockExpr)
	}
}

func TestBusyPodIsNotGivenASecondRun(t *testing.T) {
	f := newFixture(t)
	f.pods.busy = true
	f.kaneo.labels["t2"] = []string{"repo:testrepo"}
	f.signed("/", move("t2", "to-do"))
	f.wantNoLaunch()
	f.wantLog(`card="t2: pod busy"`)
}

// A failed probe must not read as idle: starting a second agent on a
// live clone is the one thing the guard exists to prevent.
func TestUnansweredBusyProbeReportsAnError(t *testing.T) {
	f := newFixture(t)
	f.pods.fail = errors.New("the API server is away")
	f.kaneo.labels["t2"] = []string{"repo:testrepo"}
	f.signed("/", move("t2", "to-do"))
	f.wantNoLaunch()
	f.wantLog("reconcile error")
	f.wantLog("probe claude-session-0")

}

func TestOutsideOwnerRepoIsSkipped(t *testing.T) {
	f := newFixture(t)
	f.kaneo.labels["t2"] = []string{"repo:Other/elsewhere"}
	f.signed("/", move("t2", "to-do"))
	// A skipped card must not wake the set either: a skip costs nothing.
	f.wantUntouched()
	f.wantLog("outside allowed owner TestOwner/*")
}

// Short labels resolve under the configured owner before authorization.
func TestRepoLabelResolvesToOwnerAndName(t *testing.T) {
	f := newFixture(t)
	f.kaneo.labels["short"] = []string{"repo:alpha"}
	f.kaneo.labels["full"] = []string{"repo:Other/gamma"}
	for id, want := range map[string]string{"short": "TestOwner/alpha", "full": "Other/gamma"} {
		got, ok, err := f.d.repoForTask(t.Context(), id)
		if err != nil || !ok || got != want {
			t.Errorf("repoForTask(%s) = %q, %v, %v; want %q", id, got, ok, err, want)
		}
	}
}

func TestRepositoryScopeAllowsNewReposUnderExactOwner(t *testing.T) {
	d := &dispatcher{owner: "ChristopherScot"}
	for repo, want := range map[string]bool{
		"ChristopherScot/new-repo":         true,
		"christopherscot/kaneo-dispatcher": true,
		"ChristopherScot/.github":          true,
		"schoolhouse-world/app":            false,
		"ChristopherScot-other/repo":       false,
		"Other/ChristopherScot":            false,
		"ChristopherScot/":                 false,
		"ChristopherScot/..":               false,
		"ChristopherScot/.":                false,
		"ChristopherScot/../Other/repo":    false,
		"ChristopherScot/repo?x=y":         false,
		"ChristopherScot/*":                false,
	} {
		if got := d.repositoryAllowed(repo); got != want {
			t.Errorf("repositoryAllowed(%q)=%v, want %v", repo, got, want)
		}
	}
	if (&dispatcher{}).repositoryAllowed("ChristopherScot/repo") {
		t.Fatal("missing owner allowed work")
	}
}

func TestCardWithoutExactlyOneRepoLabelIsSkipped(t *testing.T) {
	for _, tc := range []struct {
		name   string
		labels []string
	}{
		{name: "NoLabels"},
		{name: "NoRepoLabel", labels: []string{"bug"}},
		{name: "TwoRepoLabels", labels: []string{"repo:a", "repo:b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.kaneo.labels["t2"] = tc.labels
			f.signed("/", move("t2", "to-do"))
			f.wantNoLaunch()
			f.wantLog(`card="t2: card needs exactly one repo: label"`)
		})
	}
}

// A Kaneo outage must not read as a board full of unlabelled cards.
func TestLabelLookupFailureIsAnErrorNotNoLabel(t *testing.T) {
	f := newFixture(t)
	f.kaneo.labelErr = http.StatusBadGateway
	f.signed("/", move("t2", "to-do"))
	f.wantNoLaunch()
	f.wantLog(`msg="reconcile error" card="t2: GET /api/label/task/t2`)
	f.wantLog("502")
	if strings.Contains(f.log.String(), "exactly one repo") {
		t.Error("a failed lookup was reported as a missing label")
	}
}

// The Kaneo CLI's `{"error": ...}` with exit 0 cost three separate
// fixes. A typed decode makes the same shape from the API a parse error
// rather than an empty list.
func TestNonArrayLabelAnswerIsAnError(t *testing.T) {
	f := newFixture(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/task/tasks/") {
			_, _ = io.WriteString(w, `{"data":{"columns":[{"slug":"to-do","tasks":[{"id":"t2"}]}]}}`)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/task-relation/") {
			_, _ = io.WriteString(w, `[]`)
			return
		}
		_, _ = io.WriteString(w, `{"error":"unauthorized"}`)
	}))
	t.Cleanup(srv.Close)
	f.d.kaneoURL = srv.URL
	f.signed("/", move("t2", "to-do"))
	f.wantNoLaunch()
	f.wantLog(`msg="reconcile error"`)
}

func TestPayloadWithoutATaskIdIsSkipped(t *testing.T) {
	f := newFixture(t)
	f.signed("/", `{"event":"task.status_changed","task":{},"data":{"newStatus":"to-do"}}`)
	f.wantNoLaunch()
	f.wantLog(`reason="payload names no task id"`)
}

func TestWebhookAcceptsAnyPath(t *testing.T) {
	f := newFixture(t)
	f.kaneo.labels["t2"] = []string{"repo:testrepo"}
	if w := f.signed("/kaneo/hook", move("t2", "to-do")); w.Code != http.StatusAccepted {
		t.Errorf("POST /kaneo/hook = %d, want 202", w.Code)
	}
	if len(f.pods.launches()) != 1 {
		t.Error("a delivery on a sub-path did not dispatch")
	}
}

func TestNonzeroLaunchIsAnError(t *testing.T) {
	f := newFixture(t)
	f.kaneo.tasks["t1"] = map[string]any{"id": "t1", "projectId": "proj", "status": authorColumn}
	f.d.exec = func(ctx context.Context, pod string, argv []string) (int, error) {
		if argv[2] == launchLine {
			return 7, nil
		}
		return 0, nil
	}
	if err := f.d.runStage(t.Context(), "TestOwner/testrepo", routes[authorColumn], card{ID: "t1"}, authorColumn); err == nil || !strings.Contains(err.Error(), "exited 7") {
		t.Fatalf("launch error=%v", err)
	}
}

func TestWorkChangedDuringWakeDoesNotLaunch(t *testing.T) {
	for _, change := range []string{"moved", "started"} {
		t.Run(change, func(t *testing.T) {
			f := newFixture(t)
			f.kaneo.labels["t1"] = []string{"repo:testrepo"}
			f.d.wake = func(context.Context) error {
				f.kaneo.mu.Lock()
				defer f.kaneo.mu.Unlock()
				if change == "moved" {
					f.kaneo.tasks["t1"]["status"] = "backlog"
				} else {
					f.kaneo.activity["t1"] = []map[string]string{{"type": "comment", "content": "Work started: stage-author", "createdAt": ago(time.Minute)}}
				}
				return nil
			}
			f.signed("/", move("t1", "to-do"))
			f.wantNoLaunch()
		})
	}
}
