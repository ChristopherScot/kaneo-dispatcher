package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

const workflowMap = "kaneo-workflows"
const manualLabel = "workflow:manual"

// The plan is fixed when the parent is activated. A move is claimed with a
// ConfigMap CAS before calling Kaneo, whose PUT API has no conditional update.
// A crash or timeout after that claim must be observed or recovered manually;
// replaying an uncertain PUT could overwrite a worker's newer status.
type workflow struct {
	Order        []string      `json:"order"`
	Index        int           `json:"index"`
	Phase        workflowPhase `json:"phase"`
	BacklogSince time.Time     `json:"backlogSince"`
	Pending      *workflowMove `json:"pending,omitempty"`
}

type workflowMove struct {
	Task  string    `json:"task"`
	From  string    `json:"from"`
	To    string    `json:"to"`
	Visit time.Time `json:"visit"`
}

// Observations are live facts, not persisted states. In particular, a claimed
// move can still be waiting for an API response or need manual recovery.
type workflowObservation struct {
	ParentID, ParentStatus     string
	ParentVisit                time.Time
	ChildStatus                string
	ChildVisit, NextChildVisit time.Time
	ChildDone                  bool
	PendingStatus              string
	PendingVisit               time.Time
}

type workflowDecision struct {
	State   workflow
	Changed bool
	Move    *workflowMove
}

type taskRelation struct {
	Source string `json:"sourceTaskId"`
	Target string `json:"targetTaskId"`
	Type   string `json:"relationType"`
}

type workflowTask struct {
	ID      string `json:"id"`
	Project string `json:"projectId"`
	Status  string `json:"status"`
	Number  int    `json:"number"`
}

var errWorkflowBlocked = errors.New("workflow blocked")

func blocked(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errWorkflowBlocked, fmt.Sprintf(format, args...))
}

func (d *dispatcher) relations(ctx context.Context, id string) ([]taskRelation, error) {
	var relations []taskRelation
	err := d.kaneo(ctx, "/task-relation/"+url.PathEscape(id), &relations)
	if err == nil && relations == nil {
		err = fmt.Errorf("task relations for %s are not an array", id)
	}
	return relations, err
}

func relationChildren(relations []taskRelation, id string) []string {
	var children []string
	for _, r := range relations {
		if r.Type == "subtask" && r.Source == id {
			children = append(children, r.Target)
		}
	}
	sort.Strings(children)
	return slices.Compact(children)
}

func (d *dispatcher) workflowTask(ctx context.Context, id string) (workflowTask, error) {
	var task workflowTask
	if err := d.kaneo(ctx, "/task/"+url.PathEscape(id), &task); err != nil {
		return task, err
	}
	if task.ID != id || task.Project != d.project || task.Status == "" {
		return task, fmt.Errorf("invalid task %s or task outside configured project", id)
	}
	return task, nil
}

func (d *dispatcher) manualTask(ctx context.Context, id string) (bool, error) {
	var labels []struct {
		Name string `json:"name"`
	}
	if err := d.kaneo(ctx, "/label/task/"+url.PathEscape(id), &labels); err != nil {
		return false, err
	}
	if labels == nil {
		return false, fmt.Errorf("task labels for %s are not an array", id)
	}
	for _, label := range labels {
		if label.Name == manualLabel {
			return true, nil
		}
	}
	return false, nil
}

// completedWork requires both board completion and a merge into the repository's
// current default branch. Closing a PR, or merging it into a feature branch,
// does not release a dependent task.
func (d *dispatcher) completedWork(ctx context.Context, id string) (bool, error) {
	task, err := d.workflowTask(ctx, id)
	if err != nil || task.Status != "done" {
		return false, err
	}
	pr, err := d.mergedWork(ctx, id)
	if err != nil || pr == nil {
		return false, err
	}
	return d.mergeVerified(ctx, id, pr.Number)
}

// mergedWork returns only qualifying, non-invalidated default-branch merges.
func (d *dispatcher) mergedWork(ctx context.Context, id string) (*pullRequest, error) {
	repo, ok, err := d.repoForTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, blocked("%s needs exactly one repo: label", id)
	}
	pr, err := d.taskPullRequest(ctx, repo, id)
	if err != nil || pr == nil || pr.MergedAt == nil {
		return nil, err
	}
	cm, err := d.commandStore(ctx)
	if err != nil {
		return nil, err
	}
	if raw := cm.Data["reverted."+id]; raw != "" {
		var invalidated int
		if err := json.Unmarshal([]byte(raw), &invalidated); err != nil {
			return nil, err
		}
		if pr.Number <= invalidated {
			return nil, nil
		}
	}
	var repository struct {
		DefaultBranch string `json:"default_branch"`
	}
	if err := d.github(ctx, "/repos/"+repo, &repository); err != nil {
		return nil, err
	}
	if repository.DefaultBranch == "" {
		return nil, fmt.Errorf("repository %s has no default branch", repo)
	}
	if pr.Base.Ref != repository.DefaultBranch || !strings.EqualFold(pr.Base.Repo.FullName, repo) {
		return nil, nil
	}
	return pr, nil
}

// prerequisites returns the full dependency closure, ordered before their
// dependents. Both scheduling and launch gates use this walk, including paths
// through external tasks; neither can overlook an indirect sibling dependency.
func (d *dispatcher) prerequisites(ctx context.Context, id string) ([]string, error) {
	visiting, visited := map[string]bool{}, map[string]bool{}
	var dependencies []string
	var visit func(string) error
	visit = func(task string) error {
		if visiting[task] {
			return blocked("dependency cycle at %s", task)
		}
		if visited[task] {
			return nil
		}
		visiting[task] = true
		rels, err := d.relations(ctx, task)
		if err != nil {
			return err
		}
		for _, r := range rels {
			if r.Type == "blocks" && r.Target == task {
				if err := visit(r.Source); err != nil {
					return err
				}
			}
		}
		visiting[task] = false
		visited[task] = true
		if task != id {
			dependencies = append(dependencies, task)
		}
		return nil
	}
	err := visit(id)
	return dependencies, err
}

func (d *dispatcher) dependenciesReady(ctx context.Context, id string) error {
	dependencies, err := d.prerequisites(ctx, id)
	if err != nil {
		return err
	}
	for _, dependency := range dependencies {
		done, err := d.completedWork(ctx, dependency)
		if err != nil {
			return err
		}
		if !done {
			return blocked("waiting for %s to be Done with a merged PR", dependency)
		}
	}
	return nil
}

// Child order is a stable topological ordering; ticket number breaks ties.
// Nested or shared children are rejected until their semantics are supported.
func (d *dispatcher) workflowOrder(ctx context.Context, parent string, children []string) ([]string, error) {
	tasks := map[string]workflowTask{}
	dependencies := map[string][]string{}
	for _, id := range children {
		task, err := d.workflowTask(ctx, id)
		if err != nil {
			return nil, err
		}
		tasks[id] = task
		rels, err := d.relations(ctx, id)
		if err != nil {
			return nil, err
		}
		for _, r := range rels {
			if r.Type == "subtask" && (r.Source == id || r.Target == id && r.Source != parent) {
				return nil, blocked("nested or shared subtask %s", id)
			}
		}
		prerequisites, err := d.prerequisites(ctx, id)
		if err != nil {
			return nil, err
		}
		for _, dependency := range prerequisites {
			if slices.Contains(children, dependency) {
				dependencies[id] = append(dependencies[id], dependency)
			}
		}
	}
	remaining := slices.Clone(children)
	sort.Slice(remaining, func(i, j int) bool {
		a, b := tasks[remaining[i]], tasks[remaining[j]]
		if a.Number == b.Number {
			return a.ID < b.ID
		}
		return a.Number < b.Number
	})
	var order []string
	for len(remaining) > 0 {
		next := -1
		for i, id := range remaining {
			ready := true
			for _, dep := range dependencies[id] {
				if !slices.Contains(order, dep) {
					ready = false
				}
			}
			if ready {
				next = i
				break
			}
		}
		if next == -1 {
			return nil, blocked("subtask dependency cycle")
		}
		order = append(order, remaining[next])
		remaining = slices.Delete(remaining, next, next+1)
	}
	return order, nil
}

func loadWorkflow(ctx context.Context, maps typedcorev1.ConfigMapInterface, parent string) (*corev1.ConfigMap, *workflow, error) {
	if maps == nil {
		return nil, nil, fmt.Errorf("workflow store not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cm, err := maps.Get(ctx, workflowMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: workflowMap}, Data: map[string]string{}}, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	raw := cm.Data[parent]
	if raw == "" {
		return cm, nil, nil
	}
	var state workflow
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return nil, nil, fmt.Errorf("workflow %s: %w", parent, err)
	}
	if len(state.Order) == 0 || state.Index < 0 || state.Index > len(state.Order) {
		return nil, nil, fmt.Errorf("invalid workflow state for %s", parent)
	}
	switch state.Phase {
	case readyChild, awaitingChild:
		if state.Index == len(state.Order) {
			return nil, nil, fmt.Errorf("workflow %s has no selected child in phase %s", parent, state.Phase)
		}
	case childrenComplete:
		if state.Index != len(state.Order) {
			return nil, nil, fmt.Errorf("workflow %s has unfinished children in completed phase", parent)
		}
	default:
		return nil, nil, fmt.Errorf("workflow %s has unknown phase %q", parent, state.Phase)
	}
	return cm, &state, nil
}

// Kubernetes resourceVersion is the compare-and-swap. Conflicts stop this
// attempt before touching Kaneo; the next webhook/sweep reads the winner.
func saveState(ctx context.Context, maps typedcorev1.ConfigMapInterface, cm *corev1.ConfigMap, parent string, state any) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[parent] = string(raw)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var saved *corev1.ConfigMap
	if cm.ResourceVersion == "" {
		saved, err = maps.Create(ctx, cm, metav1.CreateOptions{})
	} else {
		saved, err = maps.Update(ctx, cm, metav1.UpdateOptions{})
	}
	if err == nil {
		cm.ResourceVersion = saved.ResourceVersion
	}
	return err
}

// parentChildren recognizes saved parents even after all their relations are
// removed. They must fail plan validation, never fall through to leaf handling.
func (d *dispatcher) parentChildren(ctx context.Context, id string) ([]string, bool, error) {
	rels, err := d.relations(ctx, id)
	if err != nil {
		return nil, false, err
	}
	children := relationChildren(rels, id)
	owner, err := d.workflowMembership(ctx, id)
	return children, len(children) > 0 || owner == id, err
}

// A parent can be paused without changing queue state. Validate its identity
// and manual gate here so orchestration and direct-child launches agree.
func (d *dispatcher) workflowParentTask(ctx context.Context, id string) (workflowTask, error) {
	task, err := d.workflowTask(ctx, id)
	if err != nil {
		return task, err
	}
	rels, err := d.relations(ctx, id)
	if err != nil {
		return task, err
	}
	for _, relation := range rels {
		if relation.Type == "subtask" && relation.Target == id {
			return task, blocked("nested parent %s", id)
		}
	}
	owner, err := d.workflowMembership(ctx, id)
	if err != nil {
		return task, err
	}
	if owner != "" && owner != id {
		return task, blocked("parent %s belongs to saved workflow %s", id, owner)
	}
	manual, err := d.manualTask(ctx, id)
	if err != nil {
		return task, err
	}
	if manual {
		return task, blocked("parent %s is manual", id)
	}
	return task, nil
}

func (d *dispatcher) checkWorkflowPlan(ctx context.Context, state *workflow, order []string) error {
	if !slices.Equal(state.Order, order) {
		return blocked("subtask plan changed; restore its relations before resuming")
	}
	for i, sibling := range state.Order {
		if i == state.Index {
			continue
		}
		if i < state.Index {
			done, err := d.completedWork(ctx, sibling)
			if err != nil {
				return err
			}
			if !done {
				return blocked("previous child %s is not Done and merged", sibling)
			}
		} else {
			task, err := d.workflowTask(ctx, sibling)
			if err != nil {
				return err
			}
			if task.Status != "backlog" {
				return blocked("later child %s must stay in Backlog", sibling)
			}
		}
	}
	return nil
}

// nextWorkflow selects FSM events from observations and proposes a durable
// state; only a caller that saves it with CAS may execute its new Move.
// An intent loaded from storage is never permission to repeat a PUT.
func nextWorkflow(ctx context.Context, state workflow, seen workflowObservation) (workflowDecision, error) {
	if state.Pending != nil {
		if seen.PendingStatus == state.Pending.From && seen.PendingVisit.Equal(state.Pending.Visit) {
			return workflowDecision{}, blocked("move of %s to %s was already attempted; verify its outcome and recover explicitly", state.Pending.Task, state.Pending.To)
		}
		state.Pending = nil // the move or a later column visit was observed
		return workflowDecision{State: state, Changed: true}, nil
	}
	claim := func(task, from, to string, visit time.Time) workflowDecision {
		move := &workflowMove{Task: task, From: from, To: to, Visit: visit}
		state.Pending = move
		return workflowDecision{State: state, Changed: true, Move: move}
	}
	if seen.ParentStatus == authorColumn {
		return claim(seen.ParentID, authorColumn, strandedColumn, seen.ParentVisit), nil
	}
	// Done means both the board status and qualifying merge were observed.
	if seen.ChildDone && state.Phase != childrenComplete {
		state.Index++
		state.BacklogSince = seen.NextChildVisit
		event := advanceChildEvent
		if state.Index == len(state.Order) {
			event = finishChildrenEvent
		}
		phase, err := state.Phase.transition(ctx, event)
		if err != nil {
			return workflowDecision{}, err
		}
		state.Phase = phase
		return workflowDecision{State: state, Changed: true}, nil
	}
	switch state.Phase {
	case readyChild:
		if seen.ChildStatus == "backlog" && !seen.ChildVisit.Equal(state.BacklogSince) {
			return workflowDecision{}, blocked("child %s changed columns; retry it explicitly", state.Order[state.Index])
		}
		phase, err := state.Phase.transition(ctx, activateChildEvent)
		if err != nil {
			return workflowDecision{}, err
		}
		state.Phase = phase
		if seen.ChildStatus != "backlog" {
			return workflowDecision{State: state, Changed: true}, nil
		}
		return claim(state.Order[state.Index], "backlog", authorColumn, seen.ChildVisit), nil
	case awaitingChild:
		if seen.ChildStatus == "backlog" {
			return workflowDecision{}, blocked("child %s returned to Backlog; retry it explicitly", state.Order[state.Index])
		}
		return workflowDecision{}, nil
	case childrenComplete:
		return claim(seen.ParentID, strandedColumn, "done", seen.ParentVisit), nil
	default:
		return workflowDecision{}, fmt.Errorf("unknown workflow phase %q", state.Phase)
	}
}

// workflowParent runs state-only transitions until it holds or performs the
// child activation/final completion. Every iteration rereads durable state;
// retries and overlapping replicas use exactly the same transition table.
func (d *dispatcher) workflowParent(ctx context.Context, id string, children []string, dry bool) (cardResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	result := cardResult{skipped, id + ": sequential workflow"}
	children = slices.Clone(children)
	slices.Sort(children)
	for {
		parent, err := d.workflowParentTask(ctx, id)
		if err != nil {
			return result, err
		}
		if parent.Status != authorColumn && parent.Status != strandedColumn {
			return result, blocked("parent %s is paused in %s", id, parent.Status)
		}
		if err := d.dependenciesReady(ctx, id); err != nil {
			return result, err
		}
		// Relations may have changed since the webhook or preceding transition.
		rels, err := d.relations(ctx, id)
		if err != nil {
			return result, err
		}
		if !slices.Equal(relationChildren(rels, id), children) {
			return result, blocked("parent %s relations changed", id)
		}
		order, err := d.workflowOrder(ctx, id, children)
		if err != nil {
			return result, err
		}
		cm, state, err := loadWorkflow(ctx, d.workflows, id)
		if err != nil {
			return result, err
		}
		newPlan := state == nil
		if newPlan {
			state = &workflow{Order: order, Phase: readyChild}
		}
		if err := d.checkWorkflowPlan(ctx, state, order); err != nil {
			return result, err
		}
		seen := workflowObservation{ParentID: id, ParentStatus: parent.Status}
		work, err := d.taskWork(ctx, id, routes[authorColumn])
		if err != nil {
			return result, err
		}
		seen.ParentVisit = work.enteredAt
		if state.Index < len(order) {
			child := order[state.Index]
			task, err := d.workflowTask(ctx, child)
			if err != nil {
				return result, err
			}
			seen.ChildStatus = task.Status
			work, err := d.taskWork(ctx, child, routes[authorColumn])
			if err != nil {
				return result, err
			}
			seen.ChildVisit = work.enteredAt
			if newPlan {
				state.BacklogSince = work.enteredAt
			}
			seen.ChildDone, err = d.completedWork(ctx, child)
			if err != nil {
				return result, err
			}
			if seen.ChildDone && state.Index+1 < len(order) {
				work, err := d.taskWork(ctx, order[state.Index+1], routes[authorColumn])
				if err != nil {
					return result, err
				}
				seen.NextChildVisit = work.enteredAt
			}
		}
		if state.Pending != nil {
			if state.Pending.Task == id {
				seen.PendingStatus, seen.PendingVisit = seen.ParentStatus, seen.ParentVisit
			} else if state.Index < len(order) && state.Pending.Task == order[state.Index] {
				seen.PendingStatus, seen.PendingVisit = seen.ChildStatus, seen.ChildVisit
			} else {
				return result, fmt.Errorf("workflow intent names a task other than its parent or current child")
			}
		}
		decision, err := nextWorkflow(ctx, *state, seen)
		if err != nil {
			return result, err
		}
		if !decision.Changed {
			return result, nil
		}
		if decision.Move != nil && decision.Move.Task != id {
			manual, err := d.manualTask(ctx, decision.Move.Task)
			if err != nil {
				return result, err
			}
			if manual {
				return result, blocked("%s requires manual work/approval (%s)", decision.Move.Task, manualLabel)
			}
			if err := d.dependenciesReady(ctx, decision.Move.Task); err != nil {
				return result, err
			}
		}
		if dry {
			return cardResult{skipped, id + ": sequential workflow [dry-run]"}, nil
		}
		if err := saveState(ctx, d.workflows, cm, id, &decision.State); err != nil {
			return result, err
		}
		if decision.Move == nil {
			continue
		}
		move := decision.Move
		event := "child-queued"
		if move.Task == id {
			event = "parent-started"
			if move.To == "done" {
				event = "parent-completed"
			}
		}
		ok, err := d.moveTicket(ctx, move.Task, move.From, event, move.Visit, false)
		if err != nil {
			return result, err
		}
		if !ok {
			return result, errCardChanged
		}
		if move.To == "done" {
			return cardResult{completed, id + ": all subtasks Done and merged"}, nil
		}
		if move.Task != id {
			return cardResult{redispatched, id + ": activated " + move.Task}, nil
		}
	}
}

// workflowGate is used both before waking and immediately before launching.
// It also applies to direct child drags, so a webhook cannot bypass the queue.
func (d *dispatcher) workflowGate(ctx context.Context, id string) error {
	manual, err := d.manualTask(ctx, id)
	if err != nil {
		return err
	}
	if manual {
		return blocked("%s requires manual work/approval (%s)", id, manualLabel)
	}
	if err := d.dependenciesReady(ctx, id); err != nil {
		return err
	}
	rels, err := d.relations(ctx, id)
	if err != nil {
		return err
	}
	var parent string
	for _, r := range rels {
		if r.Type != "subtask" {
			continue
		}
		if r.Source == id {
			return blocked("parent %s cannot run a worker stage", id)
		}
		if r.Target == id {
			if parent != "" && parent != r.Source {
				return blocked("shared subtask %s", id)
			}
			parent = r.Source
		}
	}
	savedParent, err := d.workflowMembership(ctx, id)
	if err != nil {
		return err
	}
	if savedParent == id {
		return blocked("parent %s cannot run a worker stage", id)
	}
	if savedParent != "" && savedParent != parent {
		return blocked("saved subtask %s was detached from parent %s", id, savedParent)
	}
	if parent == "" {
		return nil
	}
	task, err := d.workflowParentTask(ctx, parent)
	if err != nil {
		return err
	}
	if task.Status != strandedColumn {
		return blocked("parent %s is not In Progress", parent)
	}
	_, state, err := loadWorkflow(ctx, d.workflows, parent)
	if err != nil {
		return err
	}
	if state == nil || state.Index >= len(state.Order) || state.Order[state.Index] != id {
		return blocked("%s is not the selected child of %s", id, parent)
	}
	rels, err = d.relations(ctx, parent)
	if err != nil {
		return err
	}
	order, err := d.workflowOrder(ctx, parent, relationChildren(rels, parent))
	if err != nil {
		return err
	}
	if err := d.checkWorkflowPlan(ctx, state, order); err != nil {
		return err
	}
	return d.dependenciesReady(ctx, parent)
}

// Membership survives relation deletion. Without this lookup, detaching a
// queued child would turn it into unrestricted standalone work.
func (d *dispatcher) workflowMembership(ctx context.Context, id string) (string, error) {
	cm, _, err := loadWorkflow(ctx, d.workflows, id)
	if err != nil {
		return "", err
	}
	var parent string
	for owner, raw := range cm.Data {
		var state workflow
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			return "", err
		}
		if owner != id && !slices.Contains(state.Order, id) {
			continue
		}
		if parent != "" && parent != owner {
			return "", blocked("task %s belongs to multiple saved workflows", id)
		}
		parent = owner
	}
	return parent, nil
}
