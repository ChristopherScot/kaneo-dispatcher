package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

func commandFor(t *testing.T, f *fixture, column string) *workerCommand {
	t.Helper()
	st, _ := sweeps(column)
	work, err := f.d.taskWork(t.Context(), "t1", st)
	if err != nil {
		t.Fatal(err)
	}
	command, err := f.d.issueCommand(t.Context(), "t1", "TestOwner/testrepo", column, st, work)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func reportTo(t *testing.T, f *fixture, command *workerCommand, event string) int {
	t.Helper()
	raw, err := json.Marshal(workerReport{Task: "t1", Command: command.ID, PromptDigest: command.Prompt.Digest, Event: event})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/worker-events", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer testkey")
	response := httptest.NewRecorder()
	f.d.mux().ServeHTTP(response, req)
	f.d.inflight.Wait()
	if response.Code == 200 {
		var ack struct {
			Version              int
			Accepted             bool
			Command, Task, Event string
		}
		if err := json.Unmarshal(response.Body.Bytes(), &ack); err != nil || !ack.Accepted || ack.Version != 1 || ack.Command != command.ID || ack.Task != "t1" || ack.Event != event {
			t.Fatalf("invalid acknowledgment: %s", response.Body.String())
		}
	}
	return response.Code
}

func TestCommandLifecycleAndStaleReports(t *testing.T) {
	f := newFixture(t)
	f.stale("to-do")
	f.kaneo.board = nil
	command := commandFor(t, f, "to-do")
	if got := reportTo(t, f, command, "author-started"); got != 200 {
		t.Fatalf("start: %d", got)
	}
	if f.kaneo.tasks["t1"]["status"] != "in-progress" {
		t.Fatal("start did not move card")
	}
	work, err := f.d.taskWork(t.Context(), "t1", routes[authorColumn])
	if err != nil {
		t.Fatal(err)
	}
	work, err = f.d.commandWork(t.Context(), "t1", routes[authorColumn], work)
	if err != nil || work.startedAt.IsZero() {
		t.Fatalf("no durable start: %+v %v", work, err)
	}
	if got := reportTo(t, f, command, "author-started"); got != 409 {
		t.Fatalf("duplicate start: %d", got)
	}
	if got := reportTo(t, f, command, "test-succeeded"); got != 409 {
		t.Fatalf("wrong stage: %d", got)
	}
	f.github.prs["TestOwner:fix/t1"] = openPR("abc")
	if got := reportTo(t, f, command, "author-succeeded"); got != 200 {
		t.Fatalf("outcome: %d", got)
	}
	cm, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	next, err := readCommand(cm, "t1")
	if err != nil || next == nil || next.Stage != "test" {
		t.Fatalf("next stage not commanded: %+v %v", next, err)
	}
	if got := reportTo(t, f, command, "author-failed"); got != 409 {
		t.Fatalf("obsolete command: %d", got)
	}
	if got := reportTo(t, f, next, "test-started"); got != 200 {
		t.Fatalf("next start: %d", got)
	}
	// Same column, different visit: a drag away and back revokes the old run.
	f.kaneo.activity["t1"] = append(f.kaneo.activity["t1"], map[string]string{"type": "status_changed", "createdAt": time.Now().Add(time.Second).Format(time.RFC3339Nano)})
	if got := reportTo(t, f, next, "test-succeeded"); got != 409 {
		t.Fatalf("stale visit accepted: %d", got)
	}
	if f.kaneo.tasks["t1"]["status"] != "in-review" {
		t.Fatal("stale result moved card")
	}
}

func TestCommandIssuanceHasOneWinner(t *testing.T) {
	f := newFixture(t)
	f.stale("to-do")
	work, err := f.d.taskWork(t.Context(), "t1", routes[authorColumn])
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for range cap(results) {
		wg.Go(func() {
			_, err := f.d.issueCommand(context.Background(), "t1", "TestOwner/testrepo", "to-do", routes[authorColumn], work)
			results <- err
		})
	}
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("issued %d commands", wins)
	}
}

func TestCommandMetadataCASPreservesOtherCards(t *testing.T) {
	f := newFixture(t)
	f.stale("to-do")
	command := commandFor(t, f, "to-do")
	cm, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	other, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	otherCommand := *command
	otherCommand.Stage = "test"
	if err := f.d.saveCommandRecord(t.Context(), other, "other", &otherCommand); err != nil {
		t.Fatal(err)
	}
	command.Phase = "running"
	if err := f.d.saveCommandRecord(t.Context(), cm, "t1", command); err != nil {
		t.Fatalf("unrelated CAS conflict stranded command: %v", err)
	}
	current, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if current.Data["other"] == "" {
		t.Fatal("lost other card")
	}
	// A change to this command must not be retried as though unrelated.
	command.Phase = "finished"
	if err := f.d.saveCommandRecord(t.Context(), current, "t1", command); err != nil {
		t.Fatal(err)
	}
	if err := f.d.saveCommandRecord(t.Context(), cm, "t1", command); err == nil {
		t.Fatal("overwrote a competing command change")
	}
	if err := f.d.saveCommandRecord(t.Context(), cm, "reverted.t1", 1, "t1"); err == nil {
		t.Fatal("accepted dependent write after its command changed")
	}
}

func TestUncertainMoveIsNotReplayed(t *testing.T) {
	f := newFixture(t)
	f.stale("to-do")
	command := commandFor(t, f, "to-do")
	f.kaneo.putStatus = 500
	if got := reportTo(t, f, command, "author-started"); got != 409 {
		t.Fatalf("failed PUT: %d", got)
	}
	f.kaneo.putStatus = 0
	if got := reportTo(t, f, command, "author-started"); got != 409 {
		t.Fatalf("replayed start: %d", got)
	}
	if f.kaneo.puts != 1 {
		t.Fatalf("uncertain PUT replayed %d times", f.kaneo.puts)
	}
}

func TestUnverifiedMergeNeverAgesIntoCompletion(t *testing.T) {
	f := newFixture(t)
	f.stale("mergeable")
	command := commandFor(t, f, "mergeable")
	command.Phase, command.StartedAt, command.MergePR = "running", time.Now().Add(-5*time.Hour), 1
	cm, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := f.d.saveCommandRecord(t.Context(), cm, "t1", command); err != nil {
		t.Fatal(err)
	}
	f.mergeTask("t1", "main")
	f.kaneo.tasks["t1"]["status"] = "mergeable"
	result := f.reconcile(false)
	if len(result.completed) != 0 || f.kaneo.puts != 0 {
		t.Fatalf("unverified merge completed: %+v", result)
	}
	f.kaneo.tasks["t1"]["status"] = "done"
	if done, err := f.d.completedWork(t.Context(), "t1"); err != nil || done {
		t.Fatalf("unverified dependency released: %v %v", done, err)
	}
}

func TestRevertIntentSurvivesLostOutcome(t *testing.T) {
	f := newFixture(t)
	f.stale("mergeable")
	command := commandFor(t, f, "mergeable")
	if got := reportTo(t, f, command, "merge-started"); got != 200 {
		t.Fatalf("start: %d", got)
	}
	pr := openPR("abc")[0]
	pr["base"] = map[string]any{"ref": "main", "repo": map[string]string{"full_name": "TestOwner/testrepo"}}
	f.github.prs["TestOwner:fix/t1"] = []map[string]any{pr}
	report := workerReport{Task: "t1", Command: command.ID, PromptDigest: command.Prompt.Digest, Event: "merge-authorize", PR: 1, SHA: "abc"}
	if err := f.d.acceptReport(t.Context(), report); err != nil {
		t.Fatal(err)
	}
	pr["merged_at"] = "2026-11-15T00:00:00Z"
	report.Event = "merge-authorize-revert"
	if err := f.d.acceptReport(t.Context(), report); err != nil {
		t.Fatal(err)
	}
	// The pod may now revert and die before reporting. Persisted intent alone
	// must prevent the original merge from satisfying either completion gate.
	if merged, err := f.d.mergedWork(t.Context(), "t1"); err != nil || merged != nil {
		t.Fatalf("invalidated merge accepted: %+v %v", merged, err)
	}
	f.kaneo.tasks["t1"]["status"] = "done"
	if done, err := f.d.completedWork(t.Context(), "t1"); err != nil || done {
		t.Fatalf("reverted dependency released: %v %v", done, err)
	}
}

// Inject a column change or unrelated metadata conflict at the exact boundary
// between the command claim and acknowledgment, without replaying any PUT.
type commandStoreHook struct {
	typedcorev1.ConfigMapInterface
	beforeUpdate func(*corev1.ConfigMap) error
}

func (s commandStoreHook) Update(ctx context.Context, cm *corev1.ConfigMap, options metav1.UpdateOptions) (*corev1.ConfigMap, error) {
	if err := s.beforeUpdate(cm); err != nil {
		return nil, err
	}
	return s.ConfigMapInterface.Update(ctx, cm, options)
}
func TestStartAcknowledgmentRace(t *testing.T) {
	for _, changeVisit := range []bool{true, false} {
		t.Run(map[bool]string{true: "card moved", false: "unrelated CAS conflict"}[changeVisit], func(t *testing.T) {
			f := newFixture(t)
			f.stale("in-review")
			command := commandFor(t, f, "in-review")
			injected := false
			f.d.workflows = commandStoreHook{f.d.workflows, func(cm *corev1.ConfigMap) error {
				current, err := readCommand(cm, "t1")
				if err != nil {
					return err
				}
				if current == nil || injected {
					return nil
				}
				if changeVisit && current.Phase == "starting" {
					injected = true
					f.kaneo.mu.Lock()
					f.kaneo.activity["t1"] = append(f.kaneo.activity["t1"], map[string]string{"type": "status_changed", "createdAt": time.Now().UTC().Format(time.RFC3339Nano)})
					f.kaneo.mu.Unlock()
				} else if !changeVisit && current.Phase == "running" {
					injected = true
					return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, commandMap, errors.New("unrelated writer"))
				}
				return nil
			}}
			err := f.d.acceptReport(t.Context(), workerReport{Task: "t1", Command: command.ID, PromptDigest: command.Prompt.Digest, Event: "test-started"})
			if !injected {
				t.Fatal("race was not exercised")
			}
			if changeVisit && !errors.Is(err, errCardChanged) {
				t.Fatalf("adopted later visit: %v", err)
			}
			if !changeVisit && err != nil {
				t.Fatalf("metadata conflict stranded start: %v", err)
			}
		})
	}
}

// Historical rework receipts still count after workers stop owning their wording.
func TestStructuredReworkReceipt(t *testing.T) {
	var content string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/comment/t1" {
			t.Errorf("path %s", r.URL.Path)
		}
		var body struct{ Content string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		content = body.Content
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	d := dispatcher{kaneoURL: server.URL, client: server.Client()}
	if err := d.workerArtifact(t.Context(), workerReport{Task: "t1", Event: "note", NoteKind: "rework", Message: "fix the failing check"}); err != nil {
		t.Fatal(err)
	}
	if content != "Sent back for rework: fix the failing check" {
		t.Fatalf("historical rework format changed: %q", content)
	}
	if err := d.workerArtifact(t.Context(), workerReport{Task: "t1", Event: "note", NoteKind: "unknown", Message: "not a receipt"}); err == nil {
		t.Fatal("unknown note kind accepted")
	}
}

// Inject the same interleaving as another request receiving a 429 between
// saving a move intent and sending its PUT.
type cooldownOnMoveStore struct {
	typedcorev1.ConfigMapInterface
	d    *dispatcher
	once sync.Once
}

func (s *cooldownOnMoveStore) Update(ctx context.Context, cm *corev1.ConfigMap, opts metav1.UpdateOptions) (*corev1.ConfigMap, error) {
	saved, err := s.ConfigMapInterface.Update(ctx, cm, opts)
	if err == nil && cm.Data["move.t1"] != "" {
		s.once.Do(func() { s.d.kaneoLimit.observe(time.Now().Add(time.Minute)) })
	}
	return saved, err
}

func TestLocallyDeferredMoveReleasesOwnedClaims(t *testing.T) {
	f := newFixture(t)
	f.stale("to-do")
	command := commandFor(t, f, "to-do")
	f.d.workflows = &cooldownOnMoveStore{ConfigMapInterface: f.d.workflows, d: f.d}
	report := workerReport{Task: "t1", Command: command.ID, PromptDigest: command.Prompt.Digest, Event: "author-started"}
	var deferred *kaneoDeferredError
	if err := f.d.acceptReport(t.Context(), report); !errors.As(err, &deferred) {
		t.Fatalf("expected local deferral, got %v", err)
	}
	cm, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	current, err := readCommand(cm, "t1")
	if err != nil || current.Phase != "issued" || f.kaneo.puts != 0 {
		t.Fatalf("unsent move stranded command: command=%+v puts=%d err=%v", current, f.kaneo.puts, err)
	}
	// End the test-owned cooldown explicitly; correctness must not depend on
	// how quickly CI reaches the first PUT admission check.
	f.d.kaneoLimit.mu.Lock()
	f.d.kaneoLimit.until = time.Time{}
	f.d.kaneoLimit.mu.Unlock()
	if err := f.d.acceptReport(t.Context(), report); err != nil {
		t.Fatalf("fresh retry did not recover: %v", err)
	}
	if f.kaneo.puts != 1 || f.kaneo.tasks["t1"]["status"] != "in-progress" {
		t.Fatalf("expected exactly one successful move, got %d", f.kaneo.puts)
	}
}

func TestWorkerAcceptanceRefreshesWorkflowAfterWait(t *testing.T) {
	f := workflowFixture(t)
	f.stale("to-do")
	f.kaneo.relations = []taskRelation{{"parent", "t1", "subtask"}}
	if _, err := f.d.workflowParent(t.Context(), "parent", []string{"t1"}, false); err != nil {
		t.Fatal(err)
	}
	command := commandFor(t, f, "to-do")
	activityReads := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/activity/t1" {
			activityReads++
			if activityReads == 2 { // preparation, after workflowGate succeeded
				f.kaneo.tasks["parent"]["status"] = "backlog"
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
		}
		f.kaneo.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	f.d.kaneoURL = srv.URL
	puts := f.kaneo.puts
	err := f.d.acceptReport(t.Context(), workerReport{Task: "t1", Command: command.ID, PromptDigest: command.Prompt.Digest, Event: "author-started"})
	if !errors.Is(err, errWorkflowBlocked) || f.kaneo.puts != puts || f.kaneo.tasks["t1"]["status"] != "to-do" {
		t.Fatalf("accepted child of paused parent: puts=%d err=%v", f.kaneo.puts, err)
	}
	cm, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	current, err := readCommand(cm, "t1")
	if err != nil || current.Phase != "issued" {
		t.Fatalf("claim saved before refreshed validation: command=%+v err=%v", current, err)
	}
}

func TestAuthorContractGuardsCompletion(t *testing.T) {
	for _, tc := range []struct{ mode, event string }{
		{"advance", "author-started"},
		{"advance", "author-succeeded"},
		{"blocked", "author-succeeded"},
		{"missing", "author-succeeded"},
	} {
		t.Run(tc.mode+"/"+tc.event, func(t *testing.T) {
			f := newFixture(t)
			f.stale("to-do")
			command := commandFor(t, f, "to-do")
			if tc.event == "author-succeeded" && reportTo(t, f, command, "author-started") != 200 {
				t.Fatal("start refused")
			}
			cm, err := f.d.commandStore(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			stored, err := readCommand(cm, "t1")
			if err != nil {
				t.Fatal(err)
			}
			if tc.mode == "missing" {
				stored.Prompt = nil
			} else {
				var payload promptPayload
				if err := json.Unmarshal([]byte(stored.Prompt.Payload), &payload); err != nil {
					t.Fatal(err)
				}
				payload.Mode = tc.mode
				raw, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				stored.Prompt.Template, stored.Prompt.Payload, stored.Prompt.Digest = tc.mode, string(raw), digest(string(raw))
				command.Prompt = stored.Prompt
			}
			if err := f.d.saveCommandRecord(t.Context(), cm, "t1", stored); err != nil {
				t.Fatal(err)
			}
			puts := f.kaneo.puts
			if got := reportTo(t, f, command, tc.event); got != 409 {
				t.Fatalf("invalid contract accepted: %d", got)
			}
			if f.kaneo.puts != puts {
				t.Fatal("invalid contract moved the board")
			}
			cm, err = f.d.commandStore(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			after, err := readCommand(cm, "t1")
			if err != nil || after.Phase != stored.Phase {
				t.Fatalf("invalid contract claimed command: %+v %v", after, err)
			}
		})
	}
}
