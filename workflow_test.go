package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

// Exercise the real client and resourceVersion conflicts. This store survives
// replacement of the dispatcher/client, just as the cluster's ConfigMap does.
func workflowStore(t *testing.T) typedcorev1.ConfigMapInterface {
	t.Helper()
	var mu sync.Mutex
	storedMaps := map[string]*corev1.ConfigMap{}
	version := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fail := func(code int, reason string) {
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": reason, "code": code})
		}
		name := path.Base(r.URL.Path)
		stored := storedMaps[name]
		switch r.Method {
		case http.MethodGet:
			if stored == nil {
				fail(404, "NotFound")
				return
			}
		case http.MethodPost, http.MethodPut:
			var cm corev1.ConfigMap
			if err := json.NewDecoder(r.Body).Decode(&cm); err != nil {
				t.Error(err)
				fail(400, "BadRequest")
				return
			}
			stored = storedMaps[cm.Name]
			if r.Method == http.MethodPost && stored != nil {
				fail(409, "AlreadyExists")
				return
			}
			if r.Method == http.MethodPut && (stored == nil || cm.ResourceVersion != stored.ResourceVersion) {
				fail(409, "Conflict")
				return
			}
			version++
			cm.ResourceVersion = fmt.Sprint(version)
			stored = &cm
			storedMaps[cm.Name] = stored
		default:
			fail(405, "MethodNotAllowed")
			return
		}
		_ = json.NewEncoder(w).Encode(stored)
	}))
	t.Cleanup(srv.Close)
	client, err := typedcorev1.NewForConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return client.ConfigMaps("test")
}

func workflowFixture(t *testing.T) *fixture {
	f := newFixture(t)
	for n, id := range []string{"parent", "a", "b", "c", "external"} {
		status := "backlog"
		if id == "parent" {
			status = "to-do"
		}
		f.kaneo.tasks[id] = map[string]any{"id": id, "projectId": "proj", "status": status, "number": n + 1}
		f.kaneo.labels[id] = []string{"repo:testrepo"}
	}
	for _, id := range []string{"a", "b", "c"} {
		f.kaneo.relations = append(f.kaneo.relations, taskRelation{"parent", id, "subtask"})
	}
	return f
}

func (f *fixture) mergeTask(id, base string) {
	f.kaneo.tasks[id]["status"] = "done"
	f.github.prs["TestOwner:fix/"+id] = []map[string]any{{"number": 1, "merged_at": "2026-11-15T00:00:00Z",
		"head": map[string]any{"ref": "fix/" + id, "repo": map[string]string{"full_name": "TestOwner/testrepo"}},
		"base": map[string]any{"ref": base, "repo": map[string]string{"full_name": "testowner/testrepo"}}}}
}

func TestSequentialWorkflowRecoveryAndMergeGates(t *testing.T) {
	ctx := context.Background()
	f := workflowFixture(t)
	f.kaneo.relations = append(f.kaneo.relations, taskRelation{"external", "b", "blocks"})
	f.kaneo.labels["c"] = append(f.kaneo.labels["c"], manualLabel)
	step := func(wantBlocked bool) {
		t.Helper()
		_, err := f.d.workflowParent(ctx, "parent", []string{"c", "b", "a"}, false)
		if wantBlocked && !errors.Is(err, errWorkflowBlocked) || !wantBlocked && err != nil {
			t.Fatalf("step blocked=%v: %v", wantBlocked, err)
		}
	}
	step(false)
	if f.kaneo.tasks["parent"]["status"] != "in-progress" || f.kaneo.tasks["a"]["status"] != "to-do" || f.kaneo.tasks["b"]["status"] != "backlog" {
		t.Fatal("did not activate only first child")
	}
	puts := f.kaneo.puts
	step(false) // repeated webhook/restart reads the saved selection
	if f.kaneo.puts != puts {
		t.Fatal("duplicate activation")
	}
	if err := f.d.workflowGate(ctx, "b"); !errors.Is(err, errWorkflowBlocked) {
		t.Fatalf("later child allowed: %v", err)
	}
	f.kaneo.tasks["a"]["status"] = "done"
	step(false) // Done alone cannot advance
	if f.kaneo.tasks["b"]["status"] != "backlog" {
		t.Fatal("Done without merge released next child")
	}
	f.mergeTask("a", "feature")
	step(false)
	if f.kaneo.tasks["b"]["status"] != "backlog" {
		t.Fatal("feature branch merge released next child")
	}
	f.mergeTask("a", "main")
	step(true) // external prerequisite holds b, but is never activated
	if f.kaneo.tasks["external"]["status"] != "backlog" || f.kaneo.tasks["b"]["status"] != "backlog" {
		t.Fatal("activated blocked work")
	}
	f.kaneo.tasks["b"]["status"] = "backlog"
	f.kaneo.activity["b"] = []map[string]string{{"type": "status_changed", "createdAt": time.Now().Format(time.RFC3339Nano)}}
	f.mergeTask("external", "main")
	step(true) // a manual attempt returned to Backlog while dependencies blocked
	f.kaneo.tasks["b"]["status"] = "to-do"
	step(false)
	f.kaneo.tasks["b"]["status"] = "backlog"
	f.kaneo.activity["b"] = []map[string]string{{"type": "status_changed", "createdAt": time.Now().Format(time.RFC3339Nano)}}
	step(true)
	if f.kaneo.tasks["b"]["status"] != "backlog" {
		t.Fatal("automatically retried failed child")
	}
	f.kaneo.tasks["b"]["status"] = "to-do" // explicit retry resumes the same child
	if err := f.d.workflowGate(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	f.mergeTask("b", "main")
	step(true) // manual cutover cannot be dispatched, even if dragged to Mergeable
	if err := f.d.runStage(ctx, "TestOwner/testrepo", routes["mergeable"], card{ID: "c"}, "mergeable"); !errors.Is(err, errWorkflowBlocked) {
		t.Fatalf("manual gate bypassed: %v", err)
	}
	f.wantNoLaunch()
	f.mergeTask("c", "main")
	f.kaneo.tasks["c"]["status"] = "needs-human-review"
	result, err := f.d.reconcileCard(ctx, "needs-human-review", card{ID: "c"}, false, board{}, nil)
	if err != nil || result.outcome != skipped || f.kaneo.tasks["c"]["status"] != "needs-human-review" {
		t.Fatalf("manual completion bypassed: %#v %v", result, err)
	}
	f.kaneo.tasks["c"]["status"] = "done" // human accepts the completed cutover
	step(false)
	if f.kaneo.tasks["parent"]["status"] != "done" {
		t.Fatal("parent did not complete")
	}
}

func TestWorkflowRejectsInvalidPlansAndRechecksAfterWake(t *testing.T) {
	ctx := context.Background()
	t.Run("cycle", func(t *testing.T) {
		f := workflowFixture(t)
		f.kaneo.relations = append(f.kaneo.relations, taskRelation{"a", "b", "blocks"}, taskRelation{"b", "a", "blocks"})
		_, err := f.d.workflowParent(ctx, "parent", []string{"a", "b", "c"}, false)
		if !errors.Is(err, errWorkflowBlocked) || f.kaneo.puts != 0 {
			t.Fatalf("cycle did not fail closed: %v", err)
		}
	})
	for _, mode := range []string{"paused", "manual"} {
		t.Run(mode+" during wake", func(t *testing.T) {
			f := workflowFixture(t)
			_, err := f.d.workflowParent(ctx, "parent", []string{"a", "b", "c"}, false)
			if err != nil {
				t.Fatal(err)
			}
			f.d.wake = func(context.Context) error {
				f.kaneo.mu.Lock()
				defer f.kaneo.mu.Unlock()
				if mode == "paused" {
					f.kaneo.tasks["parent"]["status"] = "backlog"
				} else {
					f.kaneo.labels["parent"] = append(f.kaneo.labels["parent"], manualLabel)
				}
				return nil
			}
			err = f.d.runStage(ctx, "TestOwner/testrepo", routes[authorColumn], card{ID: "a"}, authorColumn)
			if !errors.Is(err, errWorkflowBlocked) {
				t.Fatalf("parent %s ignored: %v", mode, err)
			}
			f.wantNoLaunch()
		})
	}
	t.Run("nested parent", func(t *testing.T) {
		f := workflowFixture(t)
		f.kaneo.tasks["outer"] = map[string]any{"id": "outer", "projectId": "proj", "status": "backlog", "number": 9}
		f.kaneo.relations = append(f.kaneo.relations, taskRelation{"outer", "parent", "subtask"})
		f.signed("/", move("parent", authorColumn))
		if f.kaneo.puts != 0 {
			t.Fatal("nested parent activated children outside its outer queue")
		}
		f.wantNoLaunch()
	})
	t.Run("plan change and out-of-order child", func(t *testing.T) {
		f := workflowFixture(t)
		_, err := f.d.workflowParent(ctx, "parent", []string{"a", "b", "c"}, false)
		if err != nil {
			t.Fatal(err)
		}
		f.kaneo.tasks["c"]["status"] = "to-do"
		if err := f.d.workflowGate(ctx, "a"); !errors.Is(err, errWorkflowBlocked) {
			t.Fatalf("out-of-order child ignored: %v", err)
		}
		f.kaneo.tasks["c"]["status"] = "backlog"
		f.kaneo.relations = slices.Delete(f.kaneo.relations, 2, 3)
		if err := f.d.workflowGate(ctx, "a"); !errors.Is(err, errWorkflowBlocked) {
			t.Fatalf("plan edit ignored: %v", err)
		}
		// A removed edge must not make that child (or the parent) standalone.
		if err := f.d.runStage(ctx, "TestOwner/testrepo", routes[authorColumn], card{ID: "c"}, authorColumn); !errors.Is(err, errWorkflowBlocked) {
			t.Fatalf("detached child allowed: %v", err)
		}
		f.kaneo.relations = nil
		if err := f.d.runStage(ctx, "TestOwner/testrepo", routes[authorColumn], card{ID: "parent"}, authorColumn); !errors.Is(err, errWorkflowBlocked) {
			t.Fatalf("detached parent allowed: %v", err)
		}
		f.mergeTask("parent", "main")
		f.kaneo.tasks["parent"]["status"] = strandedColumn
		puts := f.kaneo.puts
		_, err = f.d.reconcileCard(ctx, strandedColumn, card{ID: "parent"}, false, board{}, nil)
		if !errors.Is(err, errWorkflowBlocked) || f.kaneo.puts != puts {
			t.Fatalf("detached parent completed via leaf path: %v", err)
		}
		f.wantNoLaunch()
	})
	t.Run("child completed before first observation", func(t *testing.T) {
		f := workflowFixture(t)
		f.mergeTask("a", "main")
		_, err := f.d.workflowParent(ctx, "parent", []string{"a", "b", "c"}, false)
		if err != nil || f.kaneo.tasks["b"]["status"] != "to-do" {
			t.Fatalf("already-completed child stalled the queue: %v", err)
		}
	})
	t.Run("dry run", func(t *testing.T) {
		f := workflowFixture(t)
		_, err := f.d.workflowParent(ctx, "parent", []string{"a", "b", "c"}, true)
		if err != nil {
			t.Fatal(err)
		}
		_, state, err := loadWorkflow(ctx, f.d.workflows, "parent")
		if err != nil || state != nil || f.kaneo.puts != 0 {
			t.Fatalf("dry run mutated state: %v", err)
		}
	})
}

func TestWorkflowStoreRejectsCompetingSelections(t *testing.T) {
	ctx := context.Background()
	maps := workflowStore(t)
	cm, _, err := loadWorkflow(ctx, maps, "parent")
	if err != nil {
		t.Fatal(err)
	}
	state := &workflow{Phase: readyChild, Order: []string{"a", "b"}}
	if err := saveState(ctx, maps, cm, "parent", state); err != nil {
		t.Fatal(err)
	}
	a, first, err := loadWorkflow(ctx, maps, "parent")
	if err != nil {
		t.Fatal(err)
	}
	b, second, err := loadWorkflow(ctx, maps, "parent")
	if err != nil {
		t.Fatal(err)
	}
	first.Index = 1
	if err := saveState(ctx, maps, a, "parent", first); err != nil {
		t.Fatal(err)
	}
	if err := saveState(ctx, maps, b, "parent", second); !apierrors.IsConflict(err) {
		t.Fatalf("stale dispatcher overwrote selection: %v", err)
	}
	_, saved, err := loadWorkflow(ctx, maps, "parent")
	if err != nil || saved.Index != 1 {
		t.Fatalf("selection lost: %#v, %v", saved, err)
	}
}

func TestConcurrentActivationIssuesOnlyOneKaneoPut(t *testing.T) {
	ctx := context.Background()
	f := workflowFixture(t)
	f.kaneo.tasks["parent"]["status"] = strandedColumn
	cm, _, err := loadWorkflow(ctx, f.d.workflows, "parent")
	if err != nil {
		t.Fatal(err)
	}
	if err := saveState(ctx, f.d.workflows, cm, "parent", &workflow{Phase: readyChild, Order: []string{"a", "b", "c"}}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 2), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/api/task/a" {
			entered <- struct{}{}
			<-release
		}
		f.kaneo.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	f.d.kaneoURL = srv.URL
	first, second := make(chan error, 1), make(chan error, 1)
	run := func(out chan<- error) {
		_, err := f.d.workflowParent(ctx, "parent", []string{"a", "b", "c"}, false)
		out <- err
	}
	go run(first)
	select {
	case <-entered:
	case err := <-first:
		close(release)
		t.Fatalf("first activation did not reach PUT: %v", err)
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("activation timed out")
	}
	go run(second)
	select {
	case err := <-second:
		if !errors.Is(err, errWorkflowBlocked) {
			t.Errorf("competing activation: %v", err)
		}
	case <-entered:
		t.Error("competing dispatcher issued a second PUT")
	case <-time.After(5 * time.Second):
		t.Error("competing activation did not finish")
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	// A late duplicate can otherwise overwrite the worker's newer column.
	f.kaneo.mu.Lock()
	f.kaneo.tasks["a"]["status"] = "in-progress"
	f.kaneo.mu.Unlock()
	_, err = f.d.workflowParent(ctx, "parent", []string{"a", "b", "c"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if f.kaneo.puts != 1 || f.kaneo.tasks["a"]["status"] != "in-progress" {
		t.Fatal("activation overwrote newer worker state")
	}
}

func TestWorkflowOrdersDependenciesThroughExternalTasks(t *testing.T) {
	f := workflowFixture(t)
	f.kaneo.relations = append(f.kaneo.relations, taskRelation{"b", "external", "blocks"}, taskRelation{"external", "a", "blocks"})
	_, err := f.d.workflowParent(t.Context(), "parent", []string{"a", "b", "c"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if f.kaneo.tasks["b"]["status"] != "to-do" || f.kaneo.tasks["a"]["status"] != "backlog" || f.kaneo.tasks["external"]["status"] != "backlog" {
		t.Fatal("indirect prerequisite did not run first, or external task was activated")
	}
}

func TestMalformedLabelsCannotBypassManualGate(t *testing.T) {
	f := workflowFixture(t)
	reads := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/label/task/") {
			reads++
			w.Header().Set("Content-Type", "application/json")
			if reads == 1 {
				_, _ = w.Write([]byte(`[{"name":"repo:testrepo"},{"name":"workflow:manual"}]`))
			} else {
				_, _ = w.Write([]byte(`null`))
			}
			return
		}
		f.kaneo.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	f.d.kaneoURL = srv.URL
	f.signed("/", move("standalone", authorColumn))
	f.wantNoLaunch()
	f.wantLog("not an array")
}

func TestUncertainWorkflowMoveRequiresObservedRecovery(t *testing.T) {
	f := workflowFixture(t)
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/api/task/a" {
			attempts++
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		f.kaneo.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	f.d.kaneoURL = srv.URL
	_, err := f.d.workflowParent(t.Context(), "parent", []string{"a", "b", "c"}, false)
	if err == nil || attempts != 1 {
		t.Fatalf("activation did not fail after claiming its move: attempts=%d err=%v", attempts, err)
	}
	_, err = f.d.workflowParent(t.Context(), "parent", []string{"a", "b", "c"}, false)
	if !errors.Is(err, errWorkflowBlocked) || attempts != 1 {
		t.Fatalf("uncertain PUT was replayed: attempts=%d err=%v", attempts, err)
	}
	f.kaneo.tasks["a"]["status"] = "to-do" // explicit operator recovery, observed by the next sweep
	_, err = f.d.workflowParent(t.Context(), "parent", []string{"a", "b", "c"}, false)
	if err != nil || attempts != 1 {
		t.Fatalf("manual recovery failed: %v", err)
	}
	f.mergeTask("a", "main")
	_, err = f.d.workflowParent(t.Context(), "parent", []string{"a", "b", "c"}, false)
	if err != nil || f.kaneo.tasks["b"]["status"] != "to-do" {
		t.Fatalf("queue did not advance after recovered child merged: %v", err)
	}
}

// A real eight-child chain exhausted Kaneo's 100-request window before the
// first child could be queued. Keep the budget on the complete activation,
// including the fresh validation after moving the parent.
func TestWorkflowActivationRequestBudget(t *testing.T) {
	f := workflowFixture(t)
	f.kaneo.relations = nil
	var children []string
	for i := range 8 {
		id := fmt.Sprintf("child-%d", i)
		children = append(children, id)
		f.kaneo.tasks[id] = map[string]any{"id": id, "projectId": "proj", "status": "backlog", "number": i + 1}
		f.kaneo.labels[id] = []string{"repo:testrepo"}
		f.kaneo.relations = append(f.kaneo.relations, taskRelation{"parent", id, "subtask"})
		if i > 0 {
			f.kaneo.relations = append(f.kaneo.relations, taskRelation{children[i-1], id, "blocks"})
		}
	}
	_, err := f.d.workflowParent(t.Context(), "parent", children, false)
	if err != nil {
		t.Fatal(err)
	}
	if f.kaneo.tasks[children[0]]["status"] != "to-do" || f.kaneo.tasks[children[1]]["status"] != "backlog" {
		t.Fatal("did not activate only the first child")
	}
	if f.kaneo.hits >= 100 {
		t.Fatalf("activation used %d Kaneo requests; exhausts the real 100-request window", f.kaneo.hits)
	}
	t.Logf("eight-child activation: %d Kaneo requests", f.kaneo.hits)
}

func TestWorkflowRefreshesAfterRateLimitWait(t *testing.T) {
	f := workflowFixture(t)
	limited := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !limited && r.URL.Path == "/api/task-relation/a" && f.kaneo.tasks["parent"]["status"] == "in-progress" {
			limited = true
			// A human pauses the parent while dependency validation is waiting.
			f.kaneo.tasks["parent"]["status"] = "backlog"
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		f.kaneo.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	f.d.kaneoURL = srv.URL
	_, err := f.d.workflowParent(t.Context(), "parent", []string{"a", "b", "c"}, false)
	if !limited || !errors.Is(err, errWorkflowBlocked) || f.kaneo.tasks["a"]["status"] != "backlog" || f.kaneo.puts != 1 {
		t.Fatalf("stale validation activated child: limited=%v puts=%d err=%v", limited, f.kaneo.puts, err)
	}
	// Resuming must recover the observed parent move without a stranded claim.
	f.kaneo.tasks["parent"]["status"] = "in-progress"
	_, err = f.d.workflowParent(t.Context(), "parent", []string{"a", "b", "c"}, false)
	if err != nil || f.kaneo.tasks["a"]["status"] != "to-do" || f.kaneo.puts != 2 {
		t.Fatalf("resume stranded or replayed the move: puts=%d err=%v", f.kaneo.puts, err)
	}
}
