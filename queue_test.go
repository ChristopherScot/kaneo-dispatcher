package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func (f *fixture) enqueue(id, column, priority string, entered time.Time) {
	f.t.Helper()
	f.kaneo.tasks[id] = map[string]any{"id": id, "projectId": "proj", "status": column, "priority": priority, "createdAt": ago(24 * time.Hour)}
	f.kaneo.labels[id] = []string{"repo:testrepo"}
	f.kaneo.activity[id] = []map[string]string{{"type": "status_changed", "createdAt": entered.Format(time.RFC3339Nano)}}
	if column == "in-review" || column == "mergeable" {
		pr := openPR("abc")[0]
		pr["head"].(map[string]any)["ref"] = "fix/" + id
		f.github.prs["TestOwner:fix/"+id] = []map[string]any{pr}
	}
}

func (f *fixture) finishQueued(id string) {
	f.t.Helper()
	cm, err := f.d.commandStore(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	command, err := readCommand(cm, id)
	if err != nil || command == nil {
		f.t.Fatalf("command for %s: %+v %v", id, command, err)
	}
	command.Phase = "finished"
	if err := f.d.saveCommandRecord(f.t.Context(), cm, id, command); err != nil {
		f.t.Fatal(err)
	}
	f.kaneo.tasks[id]["status"] = "done"
}

func TestQueuePriorityAndFIFOAcrossWebhookAndSweeps(t *testing.T) {
	f := newFixture(t)
	base := time.Now().Add(-time.Minute)
	for i, priority := range []string{"no-priority", "low", "medium", "high", "urgent"} {
		f.enqueue(priority+"-new", authorColumn, priority, base.Add(time.Duration(i+20)*time.Second))
		f.enqueue(priority+"-old", authorColumn, priority, base.Add(time.Duration(i)*time.Second))
	}
	want := []string{"urgent-old", "urgent-new", "high-old", "high-new", "medium-old", "medium-new", "low-old", "low-new", "no-priority-old", "no-priority-new"}
	for i, id := range want {
		if i == 0 {
			f.signed("/", move("no-priority-new", authorColumn))
		} else {
			if s := f.reconcile(false); len(s.errors) != 0 {
				t.Fatal(s.errors)
			}
		}
		launched := f.pods.launches()
		if len(launched) != i+1 || launched[i].argv[4] != id {
			t.Fatalf("step %d: launches=%v, want next %s", i, launched, id)
		}
		f.finishQueued(id)
	}
}

func TestQueueUsesVisitNotCreationAndBreaksTiesByID(t *testing.T) {
	f := newFixture(t)
	base := time.Now().Add(-time.Minute)
	f.enqueue("returned", authorColumn, "high", base.Add(time.Second))
	f.enqueue("b", authorColumn, "high", base)
	f.enqueue("a", authorColumn, "high", base)
	f.kaneo.tasks["returned"]["createdAt"] = ago(100 * time.Hour)
	f.kaneo.activity["returned"] = append(f.kaneo.activity["returned"], map[string]string{"type": "status_changed", "createdAt": ago(10 * time.Hour)})
	for _, id := range []string{"a", "b", "returned"} {
		f.reconcile(false)
		launches := f.pods.launches()
		if len(launches) == 0 || launches[len(launches)-1].argv[4] != id {
			t.Fatalf("wanted %s next, got %v", id, launches)
		}
		f.finishQueued(id)
	}
}

func TestQueueUsesCreationWithoutMovesAndCurrentPriority(t *testing.T) {
	f := newFixture(t)
	base := time.Now().Add(-time.Minute)
	f.enqueue("created", authorColumn, "low", base)
	f.enqueue("moved", authorColumn, "high", base)
	delete(f.kaneo.activity, "created")
	f.kaneo.tasks["created"]["createdAt"] = base.Add(-time.Minute).Format(time.RFC3339Nano)
	f.kaneo.tasks["created"]["priority"] = "high"
	f.reconcile(false)
	if launches := f.pods.launches(); len(launches) != 1 || launches[0].argv[4] != "created" {
		t.Fatalf("launches=%v", launches)
	}
}

func TestQueueKeepsDependencyAndManualGates(t *testing.T) {
	f := newFixture(t)
	base := time.Now().Add(-time.Minute)
	f.enqueue("manual", authorColumn, "urgent", base)
	f.enqueue("blocked", authorColumn, "urgent", base)
	f.enqueue("ready", authorColumn, "low", base)
	f.enqueue("prerequisite", "backlog", "urgent", base)
	f.kaneo.labels["manual"] = append(f.kaneo.labels["manual"], manualLabel)
	f.kaneo.relations = []taskRelation{{"prerequisite", "blocked", "blocks"}}
	f.reconcile(false)
	if launches := f.pods.launches(); len(launches) != 1 || launches[0].argv[4] != "ready" {
		t.Fatalf("launches=%v", launches)
	}
}

func TestQueueSelectsOneCardPerStage(t *testing.T) {
	f := newFixture(t)
	base := time.Now().Add(-time.Minute)
	for column := range routes {
		f.enqueue(column+"-low", column, "low", base)
		f.enqueue(column+"-urgent", column, "urgent", base)
	}
	f.reconcile(false)
	got := []string{}
	for _, launch := range f.pods.launches() {
		got = append(got, launch.argv[4])
	}
	slices.Sort(got)
	if !slices.Equal(got, []string{"in-review-urgent", "mergeable-urgent", "to-do-urgent"}) {
		t.Fatalf("launches=%v", got)
	}
}

func TestQueueDoesNotBypassReservedStageAfterRestart(t *testing.T) {
	f := newFixture(t)
	base := time.Now().Add(-time.Minute)
	f.enqueue("first", authorColumn, "low", base)
	f.reconcile(false)
	f.enqueue("urgent", authorColumn, "urgent", base)
	restarted := &dispatcher{workflows: f.d.workflows, unclaimed: f.d.unclaimed, stale: f.d.stale}
	cm, err := restarted.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.stageAvailable(cm, "urgent", "author"); !errors.Is(err, errPodBusy) {
		t.Fatalf("restart lost reservation: %v", err)
	}
	f.reconcile(false)
	if len(f.pods.launches()) != 1 {
		t.Fatal("new urgent card bypassed an accepted command")
	}
}

func TestQueueClaimRechecksStageOnCASConflict(t *testing.T) {
	f := newFixture(t)
	a, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	b := a.DeepCopy()
	first := &workerCommand{ID: "one", Stage: "author", Phase: "issued", IssuedAt: time.Now()}
	second := &workerCommand{ID: "two", Stage: "author", Phase: "issued", IssuedAt: time.Now()}
	if err := f.d.saveCommandRecord(t.Context(), a, "a", first); err != nil {
		t.Fatal(err)
	}
	if err := f.d.saveCommandRecord(t.Context(), b, "b", second); !errors.Is(err, errPodBusy) {
		t.Fatalf("competing stage claim: %v", err)
	}
	second.Stage = "test"
	if err := f.d.saveCommandRecord(t.Context(), b, "b", second); err != nil {
		t.Fatalf("independent stage blocked: %v", err)
	}
}

func TestQueueRevalidatesVisitAndPriorityAfterWake(t *testing.T) {
	for _, change := range []string{"visit", "priority"} {
		t.Run(change, func(t *testing.T) {
			f := newFixture(t)
			f.enqueue("first", authorColumn, "high", time.Now().Add(-time.Minute))
			f.d.wake = func(context.Context) error {
				f.kaneo.mu.Lock()
				defer f.kaneo.mu.Unlock()
				if change == "priority" {
					f.kaneo.tasks["first"]["priority"] = "low"
				} else {
					f.kaneo.activity["first"] = append(f.kaneo.activity["first"], map[string]string{"type": "status_changed", "createdAt": time.Now().Format(time.RFC3339Nano)})
				}
				return nil
			}
			f.reconcile(false)
			f.wantNoLaunch()
		})
	}
}

func TestQueueMovesOnAfterRunningCardIsWithdrawn(t *testing.T) {
	f := newFixture(t)
	f.stale(authorColumn)
	f.kaneo.board = nil
	command := commandFor(t, f, authorColumn)
	if code := reportTo(t, f, command, "author-started"); code != 200 {
		t.Fatalf("start: %d", code)
	}
	f.kaneo.tasks["t1"]["status"] = "backlog"
	f.moved("t1", 0)
	f.enqueue("next", authorColumn, "low", time.Now().Add(-time.Minute))
	f.reconcile(false)
	if launches := f.pods.launches(); len(launches) != 1 || launches[0].argv[4] != "next" {
		t.Fatalf("withdrawn run held idle worker: %v", launches)
	}
}

func TestQueueDoesNotPreemptRunningWorker(t *testing.T) {
	f := newFixture(t)
	f.stale(authorColumn)
	f.kaneo.board = nil
	command := commandFor(t, f, authorColumn)
	if code := reportTo(t, f, command, "author-started"); code != 200 {
		t.Fatalf("start: %d", code)
	}
	f.pods.busy = true
	f.enqueue("urgent", authorColumn, "urgent", time.Now().Add(-time.Minute))
	f.reconcile(false)
	f.wantNoLaunch()
}

func TestQueueHoldsUnverifiedMergeEvenAfterBlockedReport(t *testing.T) {
	f := newFixture(t)
	cm, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command := &workerCommand{ID: "old", Stage: "merge", Phase: "finished", MergePR: 1, Outcome: "merge-blocked"}
	if err := f.d.saveCommandRecord(t.Context(), cm, "old", command); err != nil {
		t.Fatal(err)
	}
	if err := f.d.stageAvailable(cm, "next", "merge"); !errors.Is(err, errPodBusy) {
		t.Fatalf("unverified merge released stage: %v", err)
	}
	command.Outcome = "merge-succeeded"
	if err := f.d.saveCommandRecord(t.Context(), cm, "old", command); err != nil {
		t.Fatal(err)
	}
	if err := f.d.stageAvailable(cm, "next", "merge"); err != nil {
		t.Fatalf("verified merge held stage: %v", err)
	}
}

func TestQueueReleasesOnlyDefiniteBusyRejections(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(map[bool]string{false: "busy", true: "timeout"}[uncertain], func(t *testing.T) {
			f := newFixture(t)
			f.enqueue("first", authorColumn, "high", time.Now().Add(-time.Minute))
			f.d.exec = func(_ context.Context, _ string, argv []string) (int, error) {
				if argv[2] == launchLine {
					if uncertain {
						return 0, context.DeadlineExceeded
					}
					return 75, nil
				}
				return 0, nil
			}
			f.reconcile(false)
			cm, err := f.d.commandStore(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			command, err := readCommand(cm, "first")
			if err != nil || command == nil {
				t.Fatalf("command: %+v %v", command, err)
			}
			if uncertain {
				if command.Phase != "issued" || !errors.Is(f.d.stageAvailable(cm, "next", "author"), errPodBusy) {
					t.Fatal("uncertain launch lost reservation")
				}
			} else if command.Phase != "finished" || command.Outcome != "launch-rejected" || f.d.stageAvailable(cm, "next", "author") != nil {
				t.Fatalf("busy rejection still reserved stage: %+v", command)
			}
		})
	}
}

func TestRejectedLaunchCannotRetireStartedOrReplacementCommands(t *testing.T) {
	f := newFixture(t)
	f.stale(authorColumn)
	command := commandFor(t, f, authorColumn)
	if err := f.d.rejectLaunch(t.Context(), "t1", "obsolete"); err != nil {
		t.Fatal(err)
	}
	if code := reportTo(t, f, command, "author-started"); code != 200 {
		t.Fatalf("start: %d", code)
	}
	if err := f.d.rejectLaunch(t.Context(), "t1", command.ID); err != nil {
		t.Fatal(err)
	}
	cm, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	current, err := readCommand(cm, "t1")
	if err != nil || current == nil || current.Phase != "running" {
		t.Fatalf("retired running command: %+v %v", current, err)
	}
}

func TestLaunchRejectionFSMOnlyAcceptsIssued(t *testing.T) {
	for _, phase := range []string{"issued", "starting", "running", "reporting", "finished"} {
		state, err := fsmEvent(t.Context(), newCommandFSM(phase), "launch-rejected")
		if phase == "issued" {
			if err != nil || state != "finished" {
				t.Fatalf("issued rejection: %s %v", state, err)
			}
		} else if err == nil {
			t.Fatalf("accepted rejection from %s", phase)
		}
	}
}
