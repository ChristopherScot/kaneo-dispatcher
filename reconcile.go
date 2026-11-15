package main

// Reconciliation completes merged cards and retries stale active stages.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// sweep is what one reconcile pass found, as lines a human reads in the
// log. Returned rather than only logged, so the summary says what
// happened in one place.
type sweep struct {
	checked      int
	completed    []string
	redispatched []string
	skipped      []string
	errors       []string
}

type cardOutcome uint8

const (
	unchanged cardOutcome = iota
	completed
	redispatched
	skipped
)

type cardResult struct {
	outcome cardOutcome
	detail  string
}

// board is the part of /api/task/tasks/{project} the sweep reads.
type board struct {
	Data struct {
		Columns []struct {
			Slug  string `json:"slug"`
			Tasks []card `json:"tasks"`
		} `json:"columns"`
	} `json:"data"`
}

// The author moves cards to in-progress before working. Retry a stale card
// there through the author, which owns the resume and rework decisions.
const strandedColumn = "in-progress"

// sweeps maps active columns to the stage responsible for resuming them.
func sweeps(column string) (stage, bool) {
	if column == strandedColumn {
		return routes[authorColumn], true
	}
	st, ok := routes[column]
	return st, ok
}

// reconcile is one sweep over the columns that dispatch, plus the one
// the author strands cards in. The error is a board that could not be
// read at all; everything per card lands in the sweep's errors instead,
// so one bad card does not hide the rest.
func (d *dispatcher) reconcile(ctx context.Context, dryRun bool) (sweep, error) {
	var out sweep
	var b board
	if err := d.kaneo(ctx, "/task/tasks/"+url.PathEscape(d.project), &b); err != nil {
		return out, err
	}
	if b.Data.Columns == nil {
		return out, fmt.Errorf("board response is missing data.columns")
	}
	for _, col := range b.Data.Columns {
		if _, ok := sweeps(col.Slug); !ok && col.Slug != "needs-human-review" {
			continue // inert column: nothing was ever owed here
		}
		for _, c := range col.Tasks {
			out.checked++
			result, err := d.reconcileCard(ctx, col.Slug, c, dryRun, b)
			if err != nil {
				out.errors = append(out.errors, c.name()+": "+err.Error())
				continue
			}
			switch result.outcome {
			case completed:
				out.completed = append(out.completed, result.detail)
			case redispatched:
				out.redispatched = append(out.redispatched, result.detail)
			case skipped:
				out.skipped = append(out.skipped, result.detail)
			}
		}
	}
	return out, nil
}

// reconcileCard decides one card in a swept column: leave it, say why it
// was skipped, or start the work it is owed. Anything the sweep could not
// find out lands in errors, never in silence: an error here must not
// read as "nothing to do", because a card nobody can see is the failure
// this sweep exists for.
func (d *dispatcher) reconcileCard(ctx context.Context, column string, c card, dryRun bool, b board) (cardResult, error) {
	st, _ := sweeps(column)

	if column == "needs-human-review" {
		st = routes[authorColumn]
	}

	repo, ok, err := d.repoForTask(ctx, c.ID)
	if err != nil {
		return cardResult{}, err
	}
	if !ok {
		return cardResult{skipped, c.name() + ": card needs exactly one repo: label"}, nil
	}
	pr, err := d.taskPullRequest(ctx, repo, c.ID)
	if err != nil {
		return cardResult{}, err
	}
	if pr != nil && pr.MergedAt != nil {
		changed, err := d.completeMergedTask(ctx, c.ID, column, b, dryRun)
		if err != nil {
			return cardResult{}, err
		}
		if !changed {
			return cardResult{skipped, c.name() + ": card changed since board snapshot"}, nil
		}
		line := fmt.Sprintf("%s: PR #%d merged; %s -> done", c.name(), pr.Number, column)
		if dryRun {
			line += " [dry-run]"
		}
		return cardResult{completed, line}, nil
	}
	if column == "needs-human-review" {
		return cardResult{}, nil
	}

	work, err := d.taskWork(ctx, c.ID, st)
	if err != nil {
		return cardResult{}, err
	}
	since := work.enteredAt
	if since.IsZero() {
		// No recorded move means the card was created straight into this
		// column and never moved, so no webhook ever fired for it. Treat
		// creation as the clock.
		if c.CreatedAt == "" {
			return cardResult{}, fmt.Errorf("no recorded move and no createdAt, so nothing dates it")
		}
		since, err = time.Parse(time.RFC3339Nano, c.CreatedAt)
		if err != nil {
			return cardResult{}, fmt.Errorf("createdAt %q: %w", c.CreatedAt, err)
		}
	}
	delay := d.unclaimed
	if !work.startedAt.IsZero() {
		since = work.startedAt
		delay = d.stale
	}
	age := time.Since(since)
	if age < delay {
		return cardResult{}, nil
	}
	// The stage owns resume/rework policy. A PR or integration status may
	// exist even though its corresponding board transition never happened.
	if !d.podWorksRepo(st, repo) {
		return cardResult{skipped, c.name() + ": " + st.pod() + " does not work " + repo}, nil
	}
	line := fmt.Sprintf("%s (%s, %dh)", c.name(), column, int(age.Hours()))
	if dryRun {
		return cardResult{redispatched, line + " [dry-run]"}, nil
	}
	slog.Info("reconcile: stuck card, re-dispatching", "task", c.name(), "column", column, "since", since)
	// A wake or launch that failed is an error, not a redispatch: the
	// summary line is what a morning reads, and "redispatched 1" over a
	// card that never started is worse than silence.
	if err := d.runStage(ctx, repo, st, c, column); err != nil {
		if errors.Is(err, errPodBusy) || errors.Is(err, errCardChanged) || errors.Is(err, errWorkStarted) {
			return cardResult{skipped, c.name() + ": " + err.Error()}, nil
		}
		return cardResult{}, err
	}
	return cardResult{redispatched, line}, nil
}

// Workers post this exact marker after acquiring their lock and reaching the
// task's work. Kaneo's activity timestamps survive dispatcher/pod restarts.
const workStartedPrefix = "Work started: "

type workState struct {
	enteredAt time.Time
	startedAt time.Time
}

func (d *dispatcher) taskWork(ctx context.Context, id string, st stage) (workState, error) {
	var acts []struct {
		Type      string `json:"type"`
		CreatedAt string `json:"createdAt"`
		Content   string `json:"content"`
	}
	var work workState
	if err := d.kaneo(ctx, "/activity/"+url.PathEscape(id), &acts); err != nil {
		return work, err
	}
	marker := workStartedPrefix + path.Base(st.script)
	for _, a := range acts {
		moved := a.Type == "status_changed"
		started := a.Type == "comment" && a.Content == marker
		if !moved && !started {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, a.CreatedAt)
		if err != nil {
			return workState{}, fmt.Errorf("activity of %s: createdAt %q: %w", id, a.CreatedAt, err)
		}
		if moved && at.After(work.enteredAt) {
			work.enteredAt = at
		}
		if started && at.After(work.startedAt) {
			work.startedAt = at
		}
	}
	// A later column move starts a new attempt, even when it returns to the
	// same stage. A prior attempt must never delay picking up the new one.
	if work.startedAt.Before(work.enteredAt) {
		work.startedAt = time.Time{}
	}
	return work, nil
}

// github reads the REST API with the configured token.
func (d *dispatcher) github(ctx context.Context, p string, out any) error {
	return getJSON(ctx, d.client, d.githubURL+p, d.ghToken, out)
}

// runSweep is reconcile plus its log lines, for the handler that runs it
// after answering.
func (d *dispatcher) runSweep(ctx context.Context, dryRun bool) {
	// The HTTP trigger returns 202 before finishing; CronJob Forbid alone
	// cannot prevent the next minute's request from overlapping a slow wake.
	if !d.sweepMu.TryLock() {
		slog.Info("reconcile already running")
		return
	}
	defer d.sweepMu.Unlock()
	s, err := d.reconcile(ctx, dryRun)
	if err != nil {
		slog.Error("reconcile failed", "err", err)
		return
	}
	slog.Info("reconcile done", "checked", s.checked,
		"completed", len(s.completed), "redispatched", len(s.redispatched), "skipped", len(s.skipped), "errors", len(s.errors))
	for _, line := range s.completed {
		slog.Info("reconcile complete", "card", line)
	}
	for _, line := range s.redispatched {
		slog.Info("reconcile redispatch", "card", line)
	}
	for _, line := range s.skipped {
		slog.Info("reconcile skip", "card", line)
	}
	for _, line := range s.errors {
		slog.Error("reconcile error", "card", line)
	}
}

// Only the newest PR from this repository and exact task branch can finish
// a card. Paginate so an old merge cannot hide a newer use of the branch.
type pullRequest struct {
	Number   int     `json:"number"`
	MergedAt *string `json:"merged_at"`
	Head     struct {
		Ref  string `json:"ref"`
		Repo struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"head"`
}

func (d *dispatcher) taskPullRequest(ctx context.Context, repo, id string) (*pullRequest, error) {
	owner, _, _ := strings.Cut(repo, "/")
	branch := d.branchPrefix + id
	var newest *pullRequest
	for page := 1; ; page++ {
		q := url.Values{"head": {owner + ":" + branch}, "state": {"all"}, "per_page": {"100"}, "page": {fmt.Sprint(page)}, "sort": {"created"}, "direction": {"desc"}}
		var prs []pullRequest
		if err := d.github(ctx, "/repos/"+repo+"/pulls?"+q.Encode(), &prs); err != nil {
			return nil, err
		}
		for _, pr := range prs {
			if pr.Head.Ref == branch && strings.EqualFold(pr.Head.Repo.FullName, repo) && (newest == nil || pr.Number > newest.Number) {
				copy := pr
				newest = &copy
			}
		}
		if len(prs) < 100 {
			return newest, nil
		}
	}
}

// Kaneo requires a full task PUT. Re-read first, preserve its fields, strip
// nulls as required by the API, and verify the result. There is no conditional
// update support, so a concurrent edit between GET and PUT remains possible.
func (d *dispatcher) completeMergedTask(ctx context.Context, id, column string, b board, dry bool) (bool, error) {
	done := 0
	for _, col := range b.Data.Columns {
		if col.Slug == "done" {
			done++
		}
	}
	if done != 1 {
		return false, fmt.Errorf("project must have exactly one Done column")
	}
	p := "/task/" + url.PathEscape(id)
	var task map[string]any
	if err := d.kaneo(ctx, p, &task); err != nil {
		return false, err
	}
	if task["id"] == nil || task["projectId"] == nil || task["status"] == nil {
		return false, fmt.Errorf("task response is missing id, projectId or status")
	}
	if task["id"] != id || task["projectId"] != d.project || task["status"] != column {
		return false, nil
	}
	if dry {
		return true, nil
	}
	for k, v := range task {
		if v == nil {
			delete(task, k)
		}
	}
	task["status"] = "done"
	if err := requestJSON(ctx, d.client, http.MethodPut, d.kaneoURL+"/api"+p, d.apiKey, task, nil); err != nil {
		return false, err
	}

	var updated struct {
		ProjectID string `json:"projectId"`
		Status    string `json:"status"`
	}
	if err := d.kaneo(ctx, p, &updated); err != nil {
		return false, err
	}
	if updated.ProjectID != d.project || updated.Status != "done" {
		return false, fmt.Errorf("merged task update did not leave the card in Done")
	}
	return true, nil
}
