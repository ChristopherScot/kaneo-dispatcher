// Binary kaneo-dispatcher launches worker stages for Kaneo card movements.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

// A stage is which pod runs which script when a card lands in a column.
// The pod is named by ordinal, as the StatefulSet counts them, so a
// route cannot name a pod the wake would not cover.
type stage struct {
	ordinal int
	script  string
}

// pod is the session pod this stage runs in.
func (s stage) pod() string { return sessionSet + "-" + strconv.Itoa(s.ordinal) }

// authorColumn is the column the author stage starts from. Named because
// the sweep hands stranded cards back to this route by key.
const authorColumn = "to-do"

// routes maps dispatching columns to stages; other movements are audit-only.
var routes = map[string]stage{
	authorColumn: {0, "/usr/local/bin/stage-author"},
	"in-review":  {2, "/usr/local/bin/stage-test"},
	"mergeable":  {1, "/usr/local/bin/stage-merge"},
}

// wakeReplicas is one past the highest ordinal routes names: stages are
// pinned to pods by ordinal and a StatefulSet cannot run ordinal 2
// without 0 and 1. Derived so a fourth stage is one edit, not two.
func wakeReplicas() int {
	n := 0
	for _, st := range routes {
		n = max(n, st.ordinal+1)
	}
	return n
}

// busyProbe uses the same lock path as the workers. Exit 1 means contention.
const busyProbe = `flock -n "${NIGHTLY_LOCK:-/tmp/nightly.lock}" true`

// launchLine waits for the worker's lock acknowledgment while the accepted
// stage continues detached. It is shared verbatim with the launch tests.
//
//go:embed launch.sh
var launchLine string

// execer runs argv in a session pod and reports its exit code. An error
// means the command could not be run at all - the API server was
// unreachable, the pod is gone - which is a different thing from it
// running and exiting non-zero, and the callers treat the two
// differently.
type execer func(ctx context.Context, pod string, argv []string) (int, error)

// waker scales the session StatefulSet up to wakeReplicas and returns
// once every pod is ready, or with the reason it could not. It bounds
// itself: see wakeTimeout in kube.go.
type waker func(ctx context.Context) error

// A dispatcher holds what one deployment of this service was configured
// with. Everything is read once at startup: the secrets arrive as
// environment variables, and the Deployment is annotated to restart when
// the Secret behind them changes, so a rotated credential reaches a
// running pod by replacing it.
type dispatcher struct {
	owner        string // GitHub owner a short repo: label resolves under
	branchPrefix string // a stage branches from the task id; the sweep looks it up
	project      string // the Kaneo project the sweep reads
	kaneoURL     string
	githubURL    string
	apiKey       string // Kaneo: reads a card's labels, and the board
	secret       []byte // the HMAC key Kaneo signs deliveries with
	ghToken      string
	repos        map[string][]string // pod ordinal -> the owner/name repos it works
	stale        time.Duration       // grace period after a worker starts
	unclaimed    time.Duration       // retry delay when no worker has started
	workflows    typedcorev1.ConfigMapInterface
	kaneoLimit   kaneoCooldown
	sweepMu      sync.Mutex // prevent overlapping asynchronous sweeps
	exec         execer
	wake         waker
	client       *http.Client

	// inflight counts work started after a 202 was already sent, which
	// is everything that talks to Kaneo, GitHub or a pod. main drains it
	// on shutdown and the tests wait on it; a launched run is detached in
	// the pod and outlives this process either way.
	inflight sync.WaitGroup
}

// fromEnv is the production configuration.
func fromEnv() (*dispatcher, error) {
	// Refused at startup rather than at the first use: without any of
	// these the pod is healthy, answers every delivery, and never does the
	// right thing - a 401 to each one without the secret, a "cannot read
	// labels" for each one without the key, the wrong board for the sweep
	// without the project. A crash loop is visible; that is not.
	var missing []string
	need := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			missing = append(missing, key)
		}
		return v
	}
	secret := need("KANEO_WEBHOOK_SECRET")
	apiKey := need("KANEO_API_KEY")
	project := need("KANEO_PROJECT_ID")
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s not set; see config.yaml", strings.Join(missing, ", "))
	}
	stale, err := time.ParseDuration(envOr("RECONCILE_STALE", "4h"))
	if err != nil || stale <= 0 {
		return nil, fmt.Errorf("RECONCILE_STALE must be a positive duration")
	}
	unclaimed, err := time.ParseDuration(envOr("RECONCILE_UNCLAIMED", "15m"))
	if err != nil || unclaimed <= 0 {
		return nil, fmt.Errorf("RECONCILE_UNCLAIMED must be a positive duration")
	}
	exec, wake, workflows, err := newPodClient(envOr("DISPATCH_NS", "claude-pods"))
	if err != nil {
		return nil, err
	}
	return &dispatcher{
		// Same defaults and same variables as pod-common.sh, so a short
		// repo: label and a branch name resolve the same way here as in
		// the scripts.
		owner:        envOr("NIGHTLY_OWNER", "ChristopherScot"),
		branchPrefix: envOr("KANEO_BRANCH_PREFIX", "fix/"),
		project:      project,
		kaneoURL:     envOr("KANEO_API_URL", "http://kaneo.kaneo.svc.cluster.local:5173"),
		githubURL:    envOr("GITHUB_API_URL", "https://api.github.com"),
		apiKey:       apiKey,
		secret:       []byte(secret),
		// Legacy standalone webhooks can launch without GitHub access.
		// Workflow dependency/completion gates fail closed if it is unavailable.
		ghToken:   os.Getenv("GITHUB_TOKEN"),
		repos:     reposFromEnv(os.Environ()),
		stale:     stale,
		unclaimed: unclaimed,
		exec:      exec,
		workflows: workflows,
		wake:      wake,
		client:    &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// reposFromEnv reads REPOS_POD<n> into lists keyed by <n>. The variables
// come from the Vault-backed secret, one per session pod, as the same
// comma or whitespace separated owner/name list the pods themselves read.
func reposFromEnv(environ []string) map[string][]string {
	isSep := func(c rune) bool { return c == ',' || unicode.IsSpace(c) }
	out := map[string][]string{}
	for _, kv := range environ {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if n, found := strings.CutPrefix(k, "REPOS_POD"); found {
			out[n] = strings.FieldsFunc(v, isSep)
		}
	}
	return out
}

// card is a task as both the webhook and the board describe it.
type card struct {
	// ID is the raw task id. It is what a stage is handed, so it is what
	// names the branch (fix/<ID>) and what the sweep looks up on GitHub:
	// PR #98 is on fix/d3kgbstyuu5mmygqtorppai2, not fix/CLA-13.
	ID string `json:"id"`
	// TicketID is the CLA-13 a human reads. Display only - the webhook
	// payload does not even carry it.
	TicketID string `json:"ticketId"`
	// CreatedAt is the sweep's clock for a card that was never moved.
	CreatedAt string `json:"createdAt"`
}

// name is what the log calls the card.
func (c card) name() string {
	if c.TicketID != "" {
		return c.TicketID
	}
	if c.ID != "" {
		return c.ID
	}
	return "?"
}

// delivery is the part of a Kaneo webhook payload this reads. The task
// object carries only id, number, title, status, statusName, priority and
// url - no labels, which is why repoForTask asks the API instead.
type delivery struct {
	Event string `json:"event"`
	Task  card   `json:"task"`
	Data  struct {
		NewStatus string `json:"newStatus"`
		OldStatus string `json:"oldStatus"`
	} `json:"data"`
}

// webhook is what Kaneo posts to when a card changes column.
func (d *dispatcher) webhook(w http.ResponseWriter, r *http.Request) {
	raw, ok := d.verifiedBody(w, r)
	if !ok {
		return
	}
	// Answer before doing the work: Kaneo records delivery health, and a
	// response that waits on a pod run would read as a failed delivery.
	w.WriteHeader(http.StatusAccepted)

	var ev delivery
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("reject", "reason", "unparseable body", "err", err)
		return
	}
	if ev.Event != "task.status_changed" {
		slog.Info("ignore", "event", ev.Event)
		return
	}
	column := ev.Data.NewStatus
	previous := ev.Data.OldStatus
	if previous == "" {
		previous = column
	}
	if _, err := fsmEvent(r.Context(), newObservationFSM(previous), "observe-"+column); err != nil {
		slog.Warn("unknown board transition", "err", err)
		return
	}
	st, ok := routes[column]
	if !ok {
		// Inert columns are logged, not dropped: this is the audit trail
		// of every card movement, Chris's included.
		slog.Info("noop", "task", ev.Task.name(), "column", column)
		return
	}
	if ev.Task.ID == "" {
		slog.Warn("skip", "task", ev.Task.name(), "column", column, "reason", "payload names no task id")
		return
	}
	d.detach(r, func(ctx context.Context) { d.dispatch(ctx, column, st, ev.Task) })
}

// detach runs work after the response has gone. The context keeps the
// request's values but not its cancellation, which fires the moment the
// handler returns - before the work has made its first call.
func (d *dispatcher) detach(r *http.Request, work func(ctx context.Context)) {
	ctx := context.WithoutCancel(r.Context())
	d.inflight.Go(func() { work(ctx) })
}

// dispatch runs the stage a column implies for one card, or logs one line
// saying why not.
func (d *dispatcher) dispatch(ctx context.Context, column string, st stage, c card) {
	children, isParent, err := d.parentChildren(ctx, c.ID)
	if err != nil {
		slog.Error("read workflow", "task", c.ID, "err", err)
		return
	}
	if isParent {
		result, err := d.workflowParent(ctx, c.ID, children, false)
		slog.Info("workflow", "task", c.ID, "result", result.detail, "err", err)
		return
	}
	repo, skip, err := d.repoFor(ctx, c.ID, st)
	if err != nil {
		slog.Error("cannot read labels", "task", c.name(), "column", column, "err", err)
		return
	}
	if skip != "" {
		slog.Warn("skip", "task", c.name(), "column", column, "reason", skip)
		return
	}
	if err := d.runStage(ctx, repo, st, c, column); err != nil {
		if errors.Is(err, errPodBusy) || errors.Is(err, errCardChanged) || errors.Is(err, errWorkStarted) || errors.Is(err, errWorkflowBlocked) {
			slog.Warn("skip", "task", c.name(), "column", column, "repo", repo, "reason", err)
			return
		}
		slog.Error("dispatch failed", "task", c.name(), "column", column, "repo", repo, "err", err)
	}
}

// repoFor is the repo a card's stage should run against, or the one-line
// reason it should not run at all. The two reasons are the same for the
// webhook and the sweep, so they are decided here and only worded here.
//
// An error is a Kaneo lookup that failed, which must not read as "no
// label": saying so keeps a Kaneo outage from looking like a board full
// of unlabelled cards.
func (d *dispatcher) repoFor(ctx context.Context, id string, st stage) (repo, skip string, err error) {
	repo, ok, err := d.repoForTask(ctx, id)
	if err != nil {
		return "", "", err
	}
	if !ok {
		return "", "card needs exactly one repo: label", nil
	}
	if !d.podWorksRepo(st, repo) {
		return "", st.pod() + " does not work " + repo, nil
	}
	return repo, "", nil
}

var (
	errPodBusy     = errors.New("pod busy")
	errCardChanged = errors.New("card changed before launch")
	errWorkStarted = errors.New("worker already started this stage")
)

// runStage wakes the pods, rechecks the card and waits for worker acceptance.
func (d *dispatcher) runStage(ctx context.Context, repo string, st stage, c card, column string) error {
	if err := d.workflowGate(ctx, c.ID); err != nil {
		return err
	}
	// Wake every routed pod; the StatefulSet may be scaled to zero.
	if err := d.wake(ctx); err != nil {
		return fmt.Errorf("wake: %w", err)
	}
	// Waking can take minutes. A human may have moved the card meanwhile.
	var current struct {
		ID        string `json:"id"`
		ProjectID string `json:"projectId"`
		Status    string `json:"status"`
	}
	if err := d.kaneo(ctx, "/task/"+url.PathEscape(c.ID), &current); err != nil {
		return err
	}
	if current.ID == "" || current.ProjectID == "" || current.Status == "" {
		return fmt.Errorf("task response is missing id, projectId or status")
	}
	if current.ID != c.ID || current.ProjectID != d.project || current.Status != column {
		return errCardChanged
	}
	// A queued wake or duplicate delivery may arrive after another worker
	// started. Re-read the durable signal before trying to launch again.
	work, err := d.taskWork(ctx, c.ID, st)
	if err != nil {
		return err
	}
	if !work.startedAt.IsZero() && time.Since(work.startedAt) < d.stale {
		return errWorkStarted
	}
	if err := d.workflowGate(ctx, c.ID); err != nil {
		return err
	}
	pod := st.pod()
	busy, err := d.podIsBusy(ctx, pod)
	if err != nil {
		return err
	}
	if busy {
		return errPodBusy
	}
	command, err := d.issueCommand(ctx, c.ID, repo, column, st, work)
	if err != nil {
		return err
	}
	slog.Info("start", "task", c.name(), "repo", repo, "pod", pod, "script", path.Base(st.script))
	// The worker acknowledges its lock before continuing detached.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rc, err := d.exec(ctx, pod, []string{"sh", "-c", launchLine, st.script, c.ID, repo, command.ID})
	if err != nil {
		return fmt.Errorf("launch in %s: %w", pod, err)
	}
	if rc == 75 {
		return errPodBusy
	}
	if rc != 0 {
		return fmt.Errorf("launch in %s exited %d", pod, rc)
	}
	// "launched", not "finished": the run is detached and outlives this.
	// The log in the pod and the card are where the outcome lives.
	slog.Info("launched", "task", c.name(), "repo", repo, "rc", rc)
	return nil
}

// podIsBusy distinguishes lock contention from a failed probe. Neither
// outcome permits launching; callers report probe failures as errors.
func (d *dispatcher) podIsBusy(ctx context.Context, pod string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	rc, err := d.exec(ctx, pod, []string{"sh", "-c", busyProbe})
	if err != nil {
		return false, fmt.Errorf("probe %s: %w", pod, err)
	}
	switch rc {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("probe %s exited %d", pod, rc)
	}
}

// podWorksRepo reports whether a pod works this repo at all.
//
// routes picks a pod by column, but which repos a pod acts on is decided
// separately, by the per-pod list the session pods themselves read.
// Dispatching a repo that is not on that list runs a full job that scans
// the wrong set and exits having done nothing - a silent no-op that reads
// as a successful run. Checking here turns that into one line saying why.
//
// A pod with no list at all is refused nothing: the scripts filter by
// repo themselves, so this check only buys the better log line, and a
// missing entry says the operator has not opted that pod in rather than
// that it works nothing. An entry that is present but empty does mean
// "works nothing", and refuses everything.
func (d *dispatcher) podWorksRepo(st stage, repo string) bool {
	list, ok := d.repos[strconv.Itoa(st.ordinal)]
	if !ok {
		return true
	}
	return slices.Contains(list, repo)
}

// repoForTask is the repo a card is for, as owner/name, from its
// repo:<name> label. A label without an owner resolves under the default
// one, the same way the scripts resolve it; this is the one place that
// happens, so every later comparison and lookup sees the same string.
//
// ok is false when the card does not carry exactly one repo: label. An
// error is a lookup that failed, which is a different thing.
func (d *dispatcher) repoForTask(ctx context.Context, id string) (repo string, ok bool, err error) {
	// /api/label/task/{id}, not /api/task/{id}: the task endpoint does not
	// return labels at all.
	var labels []struct {
		Name string `json:"name"`
	}
	if err := d.kaneo(ctx, "/label/task/"+url.PathEscape(id), &labels); err != nil {
		return "", false, err
	}
	var repos []string
	for _, l := range labels {
		if r, found := strings.CutPrefix(l.Name, "repo:"); found {
			if !strings.Contains(r, "/") {
				r = d.owner + "/" + r
			}
			repos = append(repos, r)
		}
	}
	if len(repos) != 1 {
		return "", false, nil
	}
	return repos[0], true, nil
}

// kaneo GETs a path under /api and decodes the JSON it returns.
func (d *dispatcher) kaneo(ctx context.Context, p string, out any) error {
	return d.kaneoRequest(ctx, http.MethodGet, p, nil, out)
}

// getJSON reads a JSON response; callers validate endpoint-specific fields.
func getJSON(ctx context.Context, client *http.Client, u, bearer string, out any) error {
	return requestJSON(ctx, client, http.MethodGet, u, bearer, nil, out)
}
