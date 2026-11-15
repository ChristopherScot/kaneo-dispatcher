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
	Order        []string  `json:"order"`
	Index        int       `json:"index"`
	ParentVisit  string    `json:"parentVisit,omitempty"`
	BacklogSince time.Time `json:"backlogSince"`
	Activated    bool      `json:"activated"`
	Finished     bool      `json:"finished"`
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
	repo, ok, err := d.repoForTask(ctx, id)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, blocked("%s needs exactly one repo: label", id)
	}
	pr, err := d.taskPullRequest(ctx, repo, id)
	if err != nil || pr == nil || pr.MergedAt == nil {
		return false, err
	}
	var repository struct {
		DefaultBranch string `json:"default_branch"`
	}
	if err := d.github(ctx, "/repos/"+repo, &repository); err != nil {
		return false, err
	}
	if repository.DefaultBranch == "" {
		return false, fmt.Errorf("repository %s has no default branch", repo)
	}
	return pr.Base.Ref == repository.DefaultBranch && strings.EqualFold(pr.Base.Repo.FullName, repo), nil
}

// Walk all prerequisites, including external ones, to reject cycles and wait
// for their merges. External prerequisites are never activated by this queue.
func (d *dispatcher) dependenciesReady(ctx context.Context, id string) error {
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if visiting[id] {
			return blocked("dependency cycle at %s", id)
		}
		if visited[id] {
			return nil
		}
		visiting[id] = true
		rels, err := d.relations(ctx, id)
		if err != nil {
			return err
		}
		for _, r := range rels {
			if r.Type != "blocks" || r.Target != id {
				continue
			}
			if err := visit(r.Source); err != nil {
				return err
			}
			done, err := d.completedWork(ctx, r.Source)
			if err != nil {
				return err
			}
			if !done {
				return blocked("waiting for %s to be Done with a merged PR", r.Source)
			}
		}
		visiting[id] = false
		visited[id] = true
		return nil
	}
	return visit(id)
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
			if r.Type == "blocks" && r.Target == id && slices.Contains(children, r.Source) {
				dependencies[id] = append(dependencies[id], r.Source)
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
	return cm, &state, nil
}

// Kubernetes resourceVersion is the compare-and-swap. Conflicts stop this
// attempt before touching Kaneo; the next webhook/sweep reads the winner.
func saveWorkflow(ctx context.Context, maps typedcorev1.ConfigMapInterface, cm *corev1.ConfigMap, parent string, state *workflow) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[parent] = string(raw)
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

// workflowParent handles a parent without ever sending it to an author pod.
// Each board mutation is claimed durably before it is attempted.
func (d *dispatcher) workflowParent(ctx context.Context, id string, children []string, dry bool) (cardResult, error) {
	result := cardResult{skipped, id + ": sequential workflow"}
	parent, err := d.workflowTask(ctx, id)
	if err != nil {
		return result, err
	}
	if parent.Status != authorColumn && parent.Status != strandedColumn {
		return result, blocked("parent %s is paused in %s", id, parent.Status)
	}
	manual, err := d.manualTask(ctx, id)
	if err != nil {
		return result, err
	}
	if manual {
		return result, blocked("parent %s is manual", id)
	}
	if err := d.dependenciesReady(ctx, id); err != nil {
		return result, err
	}
	order, err := d.workflowOrder(ctx, id, children)
	if err != nil {
		return result, err
	}
	cm, state, err := loadWorkflow(ctx, d.workflows, id)
	if err != nil {
		return result, err
	}
	changed := state == nil
	if state == nil {
		state = &workflow{Order: order}
	}
	if !slices.Equal(state.Order, order) {
		return result, blocked("parent %s subtask plan changed; restore its relations before resuming", id)
	}
	// Completed children are checked again: reopening an earlier child pauses
	// the queue, it cannot silently permit another child to continue.
	for i, child := range state.Order {
		task, err := d.workflowTask(ctx, child)
		if err != nil {
			return result, err
		}
		if i < state.Index {
			done, err := d.completedWork(ctx, child)
			if err != nil {
				return result, err
			}
			if !done {
				return result, blocked("previous child %s is no longer complete", child)
			}
		}
		if i > state.Index && task.Status != "backlog" {
			return result, blocked("later child %s must stay in Backlog", child)
		}
	}
	if state.Index < len(order) {
		done, err := d.completedWork(ctx, order[state.Index])
		if err != nil {
			return result, err
		}
		if done {
			state.Index++
			state.Activated = false
			changed = true
		}
	}
	if changed && state.Index < len(order) {
		work, err := d.taskWork(ctx, order[state.Index], routes[authorColumn])
		if err != nil {
			return result, err
		}
		state.BacklogSince = work.enteredAt
	}
	if parent.Status == authorColumn {
		work, err := d.taskWork(ctx, id, routes[authorColumn])
		if err != nil {
			return result, err
		}
		visit := work.enteredAt.String()
		if state.ParentVisit == visit {
			return result, blocked("parent activation was already attempted; move %s to In Progress to recover", id)
		}
		state.ParentVisit = visit
		changed = true
	}
	if state.Index == len(order) {
		if state.Finished {
			return result, blocked("completion was already attempted; verify and move %s to Done manually", id)
		}
		state.Finished = true
		changed = true
	}
	if dry {
		return cardResult{skipped, id + ": sequential workflow [dry-run]"}, nil
	}
	if changed {
		if err := saveWorkflow(ctx, d.workflows, cm, id, state); err != nil {
			return result, err
		}
	}
	if parent.Status == authorColumn {
		ok, err := d.moveTask(ctx, id, authorColumn, strandedColumn, false)
		if err != nil {
			return result, err
		}
		if !ok {
			return result, errCardChanged
		}
	}
	if state.Index == len(order) {
		ok, err := d.moveTask(ctx, id, strandedColumn, "done", false)
		if err != nil {
			return result, err
		}
		if !ok {
			return result, errCardChanged
		}
		return cardResult{completed, id + ": all subtasks Done and merged"}, nil
	}
	child := order[state.Index]
	if err := d.workflowGate(ctx, child); err != nil {
		return result, err
	}
	task, err := d.workflowTask(ctx, child)
	if err != nil {
		return result, err
	}
	if task.Status != "backlog" {
		return result, nil
	}
	// A manually moved child may have run and failed before a parent sweep
	// observed it. Preserve its new Backlog visit even without our own claim.
	work, err := d.taskWork(ctx, child, routes[authorColumn])
	if err != nil {
		return result, err
	}
	if !work.enteredAt.Equal(state.BacklogSince) {
		return result, blocked("child %s changed columns; retry it explicitly", child)
	}
	if state.Activated {
		return result, blocked("child %s activation was already attempted or it returned to Backlog; retry it explicitly", child)
	}
	state.Activated = true
	if err := saveWorkflow(ctx, d.workflows, cm, id, state); err != nil {
		return result, err
	}
	ok, err := d.moveTask(ctx, child, "backlog", authorColumn, false)
	if err != nil {
		return result, err
	}
	if !ok {
		return result, errCardChanged
	}
	// The normal webhook launches it. The minute sweep recovers a missed one.
	return cardResult{redispatched, id + ": activated " + child}, nil
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
	task, err := d.workflowTask(ctx, parent)
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
	if !slices.Equal(state.Order, order) {
		return blocked("parent %s subtask plan changed", parent)
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
				return blocked("later child %s is outside Backlog", sibling)
			}
		}
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
