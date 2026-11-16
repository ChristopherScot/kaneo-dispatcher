package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/protocol"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const commandMap = "kaneo-commands"

type workerCommand struct {
	Prompt    *commandPrompt `json:"prompt,omitempty"`
	MergePR   int            `json:"mergePR,omitempty"`
	MergeSHA  string         `json:"mergeSHA,omitempty"`
	ID        string         `json:"id"`
	Repo      string         `json:"repo"`
	Stage     string         `json:"stage"`
	Phase     string         `json:"phase"`
	Column    string         `json:"column"`
	Visit     time.Time      `json:"visit"`
	IssuedAt  time.Time      `json:"issuedAt"`
	StartedAt time.Time      `json:"startedAt"`
	Outcome   string         `json:"outcome,omitempty"`
}

func (d *dispatcher) commandStore(ctx context.Context) (*corev1.ConfigMap, error) {
	if d.workflows == nil {
		return nil, fmt.Errorf("command store not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cm, err := d.workflows.Get(ctx, commandMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: commandMap}, Data: map[string]string{}}, nil
	}
	return cm, err
}

func readCommand(cm *corev1.ConfigMap, id string) (*workerCommand, error) {
	raw := cm.Data[id]
	if raw == "" {
		return nil, nil
	}
	var command workerCommand
	if err := json.Unmarshal([]byte(raw), &command); err != nil {
		return nil, err
	}
	if command.ID == "" || command.Stage == "" || command.Phase == "" {
		return nil, fmt.Errorf("invalid command for %s", id)
	}
	return &command, nil
}

func (d *dispatcher) issueCommand(ctx context.Context, id, repo, column string, st stage, work workState) (*workerCommand, error) {
	if !newTicketFSM(column).Can(strings.TrimPrefix(path.Base(st.script), "stage-") + "-started") {
		return nil, fmt.Errorf("stage cannot start in %s", column)
	}
	cm, err := d.commandStore(ctx)
	if err != nil {
		return nil, err
	}
	old, err := readCommand(cm, id)
	if err != nil {
		return nil, err
	}
	if old != nil && old.MergePR > 0 && old.Outcome != "merge-succeeded" && old.Outcome != "merge-reverted" {
		return nil, blocked("merge command requires verification or explicit recovery")
	}
	if old != nil && (old.Phase == "starting" || old.Phase == "reporting") {
		return nil, blocked("command outcome uncertain; inspect and recover %s", id)
	}
	if old != nil && old.Phase != "finished" && old.Column == column && old.Visit.Equal(work.enteredAt) {
		since, delay := old.IssuedAt, d.unclaimed
		if !old.StartedAt.IsZero() {
			since, delay = old.StartedAt, d.stale
		}
		if time.Since(since) < delay {
			return nil, errWorkStarted
		}
	}
	command := &workerCommand{ID: rand.Text(), Repo: repo, Stage: strings.TrimPrefix(path.Base(st.script), "stage-"), Phase: "issued", Column: column, Visit: work.enteredAt, IssuedAt: time.Now().UTC()}
	command.Prompt, err = d.snapshotPrompt(ctx, id, repo, command.Stage)
	if err != nil {
		return nil, err
	}
	// Snapshot reads may be slow. A changed visit must not inherit this plan.
	task, err := d.workflowTask(ctx, id)
	if err != nil {
		return nil, err
	}
	latest, err := d.taskWork(ctx, id, st)
	if err != nil {
		return nil, err
	}
	if task.Status != column || !latest.enteredAt.Equal(work.enteredAt) {
		return nil, errCardChanged
	}
	if err := d.saveCommandRecord(ctx, cm, id, command); err != nil {
		return nil, err
	}
	return command, nil
}

// A move is prepared from fresh board facts before its one-shot intent is
// claimed. No Kaneo reads (and therefore no rate-limit waits) separate that
// claim from PUT. An uncertain PUT is never replayed against the same visit.
type ticketMove struct {
	intent    workflowMove
	task      map[string]any
	attempted bool
}

func (d *dispatcher) prepareMove(ctx context.Context, id, column, event string, expectedVisit time.Time) (*ticketMove, error) {
	target, err := fsmEvent(ctx, newTicketFSM(column), event)
	if err != nil {
		return nil, err
	}
	var task map[string]any
	if err := d.kaneo(ctx, "/task/"+url.PathEscape(id), &task); err != nil {
		return nil, err
	}
	if task["id"] != id || task["projectId"] != d.project || task["status"] != column {
		return nil, errCardChanged
	}
	work, err := d.taskWork(ctx, id, stage{})
	if err != nil {
		return nil, err
	}
	if !work.enteredAt.Equal(expectedVisit) {
		return nil, errCardChanged
	}
	for k, v := range task {
		if v == nil {
			delete(task, k)
		}
	}
	task["status"] = target
	return &ticketMove{intent: workflowMove{Task: id, From: column, To: target, Visit: expectedVisit}, task: task}, nil
}

func (d *dispatcher) applyMove(ctx context.Context, move *ticketMove, dry bool) (bool, error) {
	intent := move.intent
	if dry || intent.From == intent.To {
		return true, nil
	}
	cm, err := d.commandStore(ctx)
	if err != nil {
		return false, err
	}
	var previous workflowMove
	key := "move." + intent.Task
	if raw := cm.Data[key]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &previous); err != nil {
			return false, err
		}
		if previous.From == intent.From && previous.Visit.Equal(intent.Visit) {
			return false, blocked("previous move of %s has an uncertain outcome", intent.Task)
		}
	}
	if err := d.saveCommandRecord(ctx, cm, key, &intent); err != nil {
		return false, err
	}
	move.attempted = true // conservatively retain both intents from here onward
	p := "/task/" + url.PathEscape(intent.Task)
	if err := d.kaneoRequest(ctx, http.MethodPut, p, move.task, nil); err != nil {
		var deferred *kaneoDeferredError
		if errors.As(err, &deferred) {
			// saveCommandRecord only restores this record if our claim still
			// owns it; unrelated ConfigMap writes may be safely rebased.
			if restoreErr := d.saveCommandRecord(ctx, cm, key, &previous); restoreErr != nil {
				return false, errors.Join(err, restoreErr)
			}
			move.attempted = false
		}
		return false, err
	}
	var updated struct {
		ProjectID string `json:"projectId"`
		Status    string `json:"status"`
	}
	if err := d.kaneo(ctx, p, &updated); err != nil {
		return false, err
	}
	if updated.ProjectID != d.project || updated.Status != intent.To {
		return false, fmt.Errorf("task update did not leave the card in %s", intent.To)
	}
	return true, nil
}

func (d *dispatcher) moveTicket(ctx context.Context, id, column, event string, expectedVisit time.Time, dry bool) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	_, generation := d.kaneoLimit.state()
	move, err := d.prepareMove(ctx, id, column, event, expectedVisit)
	if err != nil {
		return false, err
	}
	if _, current := d.kaneoLimit.state(); current != generation {
		return false, errKaneoObservationChanged
	}
	return d.applyMove(ctx, move, dry)
}

type workerReport = protocol.Report

func (d *dispatcher) workerEvent(w http.ResponseWriter, r *http.Request) {
	if !d.authorizedWorker(w, r) {
		return
	}
	var report workerReport
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		http.Error(w, "invalid report", 400)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF || report.Task == "" || report.Command == "" {
		http.Error(w, "invalid report", 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	if err := d.acceptReport(ctx, report); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "task": report.Task, "command": report.Command, "event": report.Event, "accepted": true})
	if strings.HasSuffix(report.Event, "-succeeded") || strings.HasSuffix(report.Event, "-failed") || report.Event == "merge-reverted" {
		// A status webhook may have arrived while the command was reporting.
		// Once its acknowledgment is durable, explicitly deliver the handoff.
		d.detach(r, func(ctx context.Context) {
			task, err := d.workflowTask(ctx, report.Task)
			if err != nil {
				slog.Error("read handoff", "task", report.Task, "err", err)
				return
			}
			if st, ok := routes[task.Status]; ok {
				d.dispatch(ctx, task.Status, st, card{ID: report.Task})
			}
		})
	}
}

func (d *dispatcher) acceptReport(ctx context.Context, report workerReport) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		err := d.acceptReportAttempt(ctx, report)
		if !errors.Is(err, errKaneoObservationChanged) {
			return err
		}
	}
}

func (d *dispatcher) acceptReportAttempt(ctx context.Context, report workerReport) error {
	_, generation := d.kaneoLimit.state()
	cm, err := d.commandStore(ctx)
	if err != nil {
		return err
	}
	command, err := readCommand(cm, report.Task)
	if err != nil {
		return err
	}
	if command == nil || command.ID != report.Command {
		return blocked("unknown or superseded command")
	}
	if report.Event != "note" && report.Event != "link" && !strings.HasPrefix(report.Event, command.Stage+"-") {
		return blocked("event does not belong to commanded stage")
	}
	task, err := d.workflowTask(ctx, report.Task)
	if err != nil {
		return err
	}
	work, err := d.taskWork(ctx, report.Task, stage{})
	if err != nil {
		return err
	}
	if task.Status != command.Column || !work.enteredAt.Equal(command.Visit) {
		return errCardChanged
	}
	repo, ok, err := d.repoForTask(ctx, report.Task)
	if err != nil {
		return err
	}
	if !ok || repo != command.Repo {
		return blocked("command repository changed")
	}
	// Recording an actual compensation must not depend on a subsequently
	// edited parent plan. Other outcomes must still pass the live plan.
	if report.Event != "merge-reverted" && report.Event != "merge-authorize-revert" && report.Event != "note" && report.Event != "link" {
		if err := d.workflowGate(ctx, report.Task); err != nil {
			return err
		}
	}
	if _, current := d.kaneoLimit.state(); current != generation {
		return errKaneoObservationChanged
	}
	switch report.Event {
	case "note", "link":
		if command.Phase != "running" && command.Phase != "finished" {
			return blocked("command has not started")
		}
		return d.workerArtifact(ctx, report)
	case "merge-authorize", "merge-authorize-revert":
		return d.authorizeGitHub(ctx, cm, report, command)
	}
	if report.Event == "merge-reverted" {
		if command.Phase != "running" {
			return blocked("only the running merge command may compensate")
		}
		if err := d.recordRevert(ctx, cm, report, command); err != nil {
			return err
		}
	}
	target, err := fsmEvent(ctx, newTicketFSM(task.Status), report.Event)
	if err != nil {
		return err
	}
	starting := report.Event == command.Stage+"-started"
	action := "report"
	if starting {
		if err := d.validatePromptStart(ctx, command, report.PromptDigest); err != nil {
			return err
		}
		action = "start"
	}

	phase, err := fsmEvent(ctx, newCommandFSM(command.Phase), action)
	if err != nil {
		return err
	}
	if report.Event == "merge-succeeded" {
		merged, err := d.mergedWork(ctx, report.Task)
		if err != nil {
			return err
		}
		if merged == nil || command.MergePR != merged.Number || command.MergeSHA != merged.Head.SHA {
			return blocked("no qualifying commanded merge")
		}
	}
	prepared, err := d.prepareMove(ctx, report.Task, task.Status, report.Event, command.Visit)
	if err != nil {
		return err
	}
	if _, current := d.kaneoLimit.state(); current != generation {
		return errKaneoObservationChanged
	}
	previous := *command
	command.Phase = phase
	if err := d.saveCommandRecord(ctx, cm, report.Task, command); err != nil {
		return err
	}
	changed, err := d.applyMove(ctx, prepared, false)
	if err != nil {
		if !prepared.attempted {
			if restoreErr := d.saveCommandRecord(ctx, cm, report.Task, &previous); restoreErr != nil {
				return errors.Join(err, restoreErr)
			}
		}
		return err
	}
	if !changed {
		return errCardChanged
	}
	// Reload after the move intent's CAS. Another writer must never be silently
	// overwritten; only this command's claim may be acknowledged.
	cm, err = d.commandStore(ctx)
	if err != nil {
		return err
	}
	current, err := readCommand(cm, report.Task)
	if err != nil {
		return err
	}
	if current == nil || current.ID != command.ID || current.Phase != phase {
		return blocked("command changed during report")
	}
	task, err = d.workflowTask(ctx, report.Task)
	if err != nil {
		return err
	}
	work, err = d.taskWork(ctx, report.Task, stage{})
	if err != nil {
		return err
	}
	if task.Status != target {
		return errCardChanged
	}
	if target == command.Column && !work.enteredAt.Equal(command.Visit) {
		return errCardChanged
	}
	if target != command.Column && !work.enteredAt.After(command.Visit) {
		return blocked("status activity not yet observed; command held")
	}
	command.Column, command.Visit = task.Status, work.enteredAt
	action = "reported"
	if starting {
		action = "started"
		command.StartedAt = time.Now().UTC()
	} else {
		command.Outcome = report.Event
		if command.Prompt != nil {
			command.Prompt.Payload = ""
		}
	}
	command.Phase, err = fsmEvent(ctx, newCommandFSM(phase), action)
	if err != nil {
		return err
	}
	return d.saveCommandRecord(ctx, cm, report.Task, command)
}

// commandWork supplements old activity receipts during rollout. The stored
// start belongs to exactly one column visit; moving away and back invalidates it.
func (d *dispatcher) commandWork(ctx context.Context, id string, st stage, work workState) (workState, error) {
	cm, err := d.commandStore(ctx)
	if err != nil {
		return work, err
	}
	command, err := readCommand(cm, id)
	if err != nil {
		return work, err
	}
	if command != nil && command.Phase == "running" && command.Stage == strings.TrimPrefix(path.Base(st.script), "stage-") && command.Visit.Equal(work.enteredAt) && command.StartedAt.After(work.startedAt) {
		work.startedAt = command.StartedAt
	}
	return work, nil
}

// Notes and links are command-scoped reports; workers never call Kaneo writes.
func (d *dispatcher) workerArtifact(ctx context.Context, report workerReport) error {
	p := "/comment/" + url.PathEscape(report.Task)
	if report.NoteKind != "" && (report.Event != "note" || report.NoteKind != "rework") {
		return fmt.Errorf("unsupported note kind")
	}
	message := report.Message
	if report.NoteKind == "rework" {
		message = reworkHeadline + ": " + message
	}
	body := map[string]string{"content": message}
	if report.Event == "link" {
		if report.URL == "" || !strings.HasPrefix(report.URL, "https://github.com/") {
			return fmt.Errorf("invalid PR URL")
		}
		p = "/external-link/task/" + url.PathEscape(report.Task)
		var existing []struct {
			URL string `json:"url"`
		}
		if err := d.kaneo(ctx, p, &existing); err != nil {
			return err
		}
		for _, link := range existing {
			if link.URL == report.URL {
				return nil
			}
		}
		body = map[string]string{"url": report.URL, "title": report.Title}
	} else if report.Message == "" {
		return fmt.Errorf("empty note")
	}
	return d.kaneoRequest(ctx, http.MethodPost, p, body, nil)
}

func (d *dispatcher) authorizeGitHub(ctx context.Context, cm *corev1.ConfigMap, report workerReport, command *workerCommand) error {
	if report.Event == "merge-authorize" && command.Phase != "running" {
		return blocked("merge command is not running")
	}
	if report.Event == "merge-authorize-revert" && command.Phase != "running" {
		return blocked("merge command is not running")
	}
	pr, err := d.taskPullRequest(ctx, command.Repo, report.Task)
	if err != nil {
		return err
	}
	if pr == nil || report.PR <= 0 || pr.Number != report.PR || report.SHA == "" || pr.Head.SHA != report.SHA {
		return blocked("PR or reviewed head changed")
	}
	var repository struct {
		DefaultBranch string `json:"default_branch"`
	}
	if err := d.github(ctx, "/repos/"+command.Repo, &repository); err != nil {
		return err
	}
	if repository.DefaultBranch == "" || pr.Base.Ref != repository.DefaultBranch || !strings.EqualFold(pr.Base.Repo.FullName, command.Repo) {
		return blocked("PR does not target the repository default branch")
	}
	if report.Event == "merge-authorize" && pr.MergedAt != nil {
		return blocked("PR already merged")
	}
	if report.Event == "merge-authorize-revert" && pr.MergedAt == nil {
		return blocked("PR was not merged")
	}
	if report.Event == "merge-authorize-revert" {
		// Persist uncertainty BEFORE the irreversible revert. A lost outcome
		// can never let the original merged PR satisfy completion again.
		if command.MergePR != pr.Number || command.MergeSHA != report.SHA {
			return blocked("merge was not authorized by this command")
		}
		return d.saveCommandRecord(ctx, cm, "reverted."+report.Task, pr.Number, report.Task)
	}
	command.MergePR, command.MergeSHA = pr.Number, report.SHA
	return d.saveCommandRecord(ctx, cm, report.Task, command)
}

// The merged original must cease satisfying reconciliation and dependencies.
// This evidence is separate from commands so issuing another command retains it.
func (d *dispatcher) recordRevert(ctx context.Context, cm *corev1.ConfigMap, report workerReport, command *workerCommand) error {
	original, err := d.taskPullRequest(ctx, command.Repo, report.Task)
	if err != nil {
		return err
	}
	if original == nil || original.MergedAt == nil || report.RevertPR <= 0 {
		return blocked("missing original/revert merge")
	}
	var revert pullRequest
	if err := d.github(ctx, fmt.Sprintf("/repos/%s/pulls/%d", command.Repo, report.RevertPR), &revert); err != nil {
		return err
	}
	if revert.Number != report.RevertPR || revert.MergedAt == nil || revert.Base.Ref != original.Base.Ref || !strings.EqualFold(revert.Base.Repo.FullName, command.Repo) || !strings.HasPrefix(revert.Body, fmt.Sprintf("Reverts #%d.", original.Number)) {
		return blocked("revert is not a merged correction of this PR")
	}
	return d.saveCommandRecord(ctx, cm, "reverted."+report.Task, original.Number, report.Task)
}

// Retry metadata conflicts caused by other cards, never side effects. A change
// to this key is a competing command and must be re-evaluated by its caller.
func (d *dispatcher) saveCommandRecord(ctx context.Context, cm *corev1.ConfigMap, key string, value any, guards ...string) error {
	expected := map[string]string{key: cm.Data[key]}
	for _, guard := range guards {
		expected[guard] = cm.Data[guard]
	}
	original := cm
	for attempt := 0; attempt < 5; attempt++ {
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		size := len(key) + len(encoded)
		priorSize := 0
		for existing, raw := range cm.Data {
			priorSize += len(existing) + len(raw)
			if existing != key {
				size += len(existing) + len(raw)
			}
		}
		limit := commandMetadataBudget
		if command, ok := value.(*workerCommand); ok && command.Phase == "issued" && command.Prompt != nil && command.Prompt.Payload != "" {
			limit = commandStoreBudget
		}
		if size > limit && size >= priorSize {
			return blocked("command store exceeds safe budget; archive finished records")
		}
		copy := cm.DeepCopy()
		err = saveState(ctx, d.workflows, copy, key, value)
		if err == nil {
			*original = *copy
			return nil
		}
		if !apierrors.IsConflict(err) && !apierrors.IsAlreadyExists(err) {
			return err
		}
		fresh, err := d.commandStore(ctx)
		if err != nil {
			return err
		}
		for guard, raw := range expected {
			if fresh.Data[guard] != raw {
				return blocked("command record changed")
			}
		}
		cm = fresh
	}
	return blocked("command store remained busy")
}

func (d *dispatcher) mergeVerified(ctx context.Context, id string, pr int) (bool, error) {
	cm, err := d.commandStore(ctx)
	if err != nil {
		return false, err
	}
	command, err := readCommand(cm, id)
	if err != nil {
		return false, err
	}
	return command == nil || command.MergePR != pr || command.Outcome == "merge-succeeded", nil
}

func (d *dispatcher) authorizedWorker(w http.ResponseWriter, r *http.Request) bool {
	if d.apiKey == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+d.apiKey)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}
