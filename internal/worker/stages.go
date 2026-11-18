package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/protocol"
)

func (r *runner) author(ctx context.Context) error {
	text, runErr := r.claude(ctx)
	// Claude and its descendants have exited. Preservation always gets its own
	// budget so a cancelled run can still publish its unfinished work, and so
	// the shutdown report below keeps its 30s even if preservation is slow.
	preserveCtx, preserveCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	pushed, checkpoint, preserveErr := r.preserveAuthor(preserveCtx)
	preserveCancel()
	ctx, cancel, runErr := shutdownBudget(ctx, runErr, 30*time.Second)
	defer cancel()
	if preserveErr != nil {
		return errors.Join(runErr, preserveErr, r.block(ctx, "Could not publish unfinished author work; the local checkout was retained: "+r.redact(preserveErr.Error())))
	}
	if checkpoint || runErr != nil {
		reason := "Author execution did not complete."
		if pushed == "" {
			reason += " No commits were made on " + r.payload.Branch + "."
		} else {
			// The dispatcher selects its resume prompt from the pushed branch.
			reason += " Unfinished work is on " + r.payload.Branch + " at " + pushed + "; moving the card to To Do resumes it from GitHub."
		}
		if checkpoint {
			reason += " Uncommitted changes were saved in a stash: checkpoint commit."
		}
		if runErr != nil {
			reason += "\n" + r.redact(runErr.Error())
		}
		return errors.Join(runErr, r.block(ctx, reason))
	}
	pr, err := r.authoredPR(ctx)
	if err != nil {
		return err
	}
	if pr.Number == 0 {
		if err := r.note(ctx, "The author produced no open pull request.\n\n"+r.evidence(text)); err != nil {
			return err
		}
		return r.outcome(ctx, "failed")
	}
	if err := r.report(ctx, protocol.Report{Event: "link", URL: pr.URL, Title: "open"}); err != nil {
		return err
	}
	return r.outcome(ctx, "succeeded")
}

// shutdownBudget replaces a cancelled ctx with one bounded by budget, so the
// cleanup and terminal report that follow a run still happen; otherwise the
// dispatcher mistakes the exited worker for active work. A run that itself
// succeeded gets the cancellation as its error, so it is never reported as
// success. An uncancelled run keeps its caller's context and budget,
// including dispatcher rate-limit waits.
//
// The session pod's preStop hook (homelab) sends SIGTERM and waits for the
// worker's lock inside a 180s termination grace period. Every budget a stage
// spends after cancellation, in sequence, must fit inside it.
func shutdownBudget(ctx context.Context, runErr error, budget time.Duration) (context.Context, context.CancelFunc, error) {
	if ctx.Err() == nil {
		return ctx, func() {}, runErr
	}
	if runErr == nil {
		runErr = ctx.Err()
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	return bounded, cancel, runErr
}

// preserveAuthor checkpoints only the commanded branch. pushed is the head it
// published, or "" when the branch holds nothing beyond its base; checkpoint
// reports whether uncommitted work was first committed as a `stash:` commit.
// Never publish a different checkout or force away work another actor pushed.
func (r *runner) preserveAuthor(ctx context.Context) (pushed string, checkpoint bool, err error) {
	branch, err := r.git(ctx, "symbolic-ref", "--quiet", "HEAD")
	if err != nil {
		return "", false, err
	}
	ref := "refs/heads/" + r.payload.Branch
	if branch != ref {
		return "", false, fmt.Errorf("author checkout changed to %q; checkpoint held", branch)
	}
	dirty, err := r.git(ctx, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return "", false, err
	}
	checkpoint = dirty != ""
	if checkpoint {
		if _, err := r.git(ctx, "add", "--all"); err != nil {
			return "", false, err
		}
		if _, err := r.git(ctx, "commit", "-m", "stash: unfinished work for "+r.cfg.Task+" ("+r.cfg.Command+")"); err != nil {
			return "", false, err
		}
	}
	ahead, err := r.git(ctx, "rev-list", "--count", r.payload.BaseSHA+".."+ref)
	if err != nil {
		return "", false, err
	}
	count, err := strconv.Atoi(ahead)
	if err != nil || count < 0 {
		return "", false, fmt.Errorf("invalid authored commit count %q", ahead)
	}
	if count == 0 {
		return "", checkpoint, nil
	}
	head, err := r.git(ctx, "rev-parse", ref)
	if err != nil {
		return "", false, err
	}
	if _, err := r.git(ctx, "push", "-u", "origin", ref+":"+ref); err != nil {
		return "", false, fmt.Errorf("preserve authored commits: %w", err)
	}
	return head, checkpoint, nil
}

type authoredPullRequest struct {
	Number              int
	URL, HeadRefName    string
	HeadRepositoryOwner struct{ Login string }
	HeadRepository      struct{ Name string }
}

func (r *runner) authoredPR(ctx context.Context) (authoredPullRequest, error) {
	var prs []authoredPullRequest
	err := r.gh(ctx, &prs, "pr", "list", "--repo", r.cfg.Repo, "--state", "open", "--head", r.payload.Branch, "--limit", "100", "--json", "number,url,headRefName,headRepositoryOwner,headRepository")
	if err != nil {
		return authoredPullRequest{}, err
	}
	var selected authoredPullRequest
	for _, pr := range prs {
		if pr.HeadRefName == r.payload.Branch && strings.EqualFold(pr.HeadRepositoryOwner.Login+"/"+pr.HeadRepository.Name, r.cfg.Repo) && pr.Number > selected.Number {
			selected = pr
		}
	}
	if selected.Number > 0 && !strings.EqualFold(selected.URL, fmt.Sprintf("https://github.com/%s/pull/%d", r.cfg.Repo, selected.Number)) {
		return authoredPullRequest{}, fmt.Errorf("unexpected authored PR identity")
	}
	return selected, nil
}

func (r *runner) test(ctx context.Context) (resultErr error) {
	text, runErr := r.claude(ctx)
	ctx, cancel, runErr := shutdownBudget(ctx, runErr, 90*time.Second)
	defer cancel()
	if err := r.verifyHead(ctx); err != nil {
		return errors.Join(err, r.block(ctx, "The test checkout changed; its verdict was refused."))
	}
	// checkout required a clean tree, so this removes only this test run's
	// scratch work. Never reset a different commit or the author/reviewer tree.
	// Scratch left behind would hold every later checkout, so the reset gets
	// its own budget rather than whatever reporting leaves of a shutdown's.
	defer func() {
		cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := r.verifyHead(cleanCtx); err != nil {
			resultErr = errors.Join(resultErr, err)
			return
		}
		_, err := r.git(cleanCtx, "reset", "--hard", "--quiet", r.payload.HeadSHA)
		if err == nil {
			_, err = r.git(cleanCtx, "clean", "-qfd")
		}
		resultErr = errors.Join(resultErr, err)
	}()
	verdict, err := readTestVerdict(filepath.Join(r.archive, "verdict.txt"))
	if err != nil || runErr != nil {
		verdict = "BLOCKED"
	}
	state := map[string]string{"PASS": "success", "FAIL": "failure", "BLOCKED": "error"}[verdict]
	if err := r.gh(ctx, nil, "api", "--method", "POST", "repos/"+r.cfg.Repo+"/statuses/"+r.payload.HeadSHA,
		"-f", "state="+state, "-f", "context=nightly/integration", "-f", "description=integration test: "+verdict); err != nil {
		return errors.Join(runErr, err)
	}
	if err := r.testLabel(ctx, verdict); err != nil {
		r.log.Print("test label: ", r.redact(err.Error()))
	}
	if err := r.comment(ctx, fmt.Sprintf("Nightly integration test: **%s** for `%s`.\n\n%s", verdict, r.payload.HeadSHA, r.evidence(text))); err != nil {
		return errors.Join(runErr, err)
	}
	switch verdict {
	case "PASS":
		state, err := r.checks(ctx)
		if err != nil {
			return err
		}
		switch state {
		case "success":
			return r.outcome(ctx, "succeeded")
		case "failure":
			return r.rework(ctx, "CI failed on "+r.payload.HeadSHA+"; see the pull request checks.")
		default:
			return fmt.Errorf("test passed but CI is %s; command held", state)
		}
	case "FAIL":
		return r.rework(ctx, "Integration testing failed on "+r.payload.HeadSHA+"; see the pull request report.")
	default:
		reason := "Integration testing could not complete on " + r.payload.HeadSHA + "; see the pull request report."
		if runErr != nil {
			reason += "\n" + r.redact(runErr.Error())
		}
		return errors.Join(runErr, r.block(ctx, reason))
	}
}

func readTestVerdict(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 33))
	if err != nil {
		return "", err
	}
	if len(raw) > 32 {
		return "", fmt.Errorf("test verdict too large")
	}
	verdict := strings.TrimSpace(string(raw))
	switch verdict {
	case "PASS", "FAIL", "BLOCKED":
		return verdict, nil
	default:
		return "", fmt.Errorf("invalid test verdict")
	}
}

func (r *runner) testLabel(ctx context.Context, verdict string) error {
	want := "tested-" + strings.ToLower(verdict)
	if err := r.gh(ctx, nil, "label", "create", want, "--repo", r.cfg.Repo, "--color", "ededed", "--description", "Integration-test verdict", "--force"); err != nil {
		return err
	}
	args := []string{"pr", "edit", strconv.Itoa(r.payload.PR), "--repo", r.cfg.Repo, "--add-label", want}
	for _, label := range []string{"tested-pass", "tested-fail", "tested-blocked"} {
		if label != want {
			args = append(args, "--remove-label", label)
		}
	}
	return r.gh(ctx, nil, args...)
}

func (r *runner) evidence(text string) string {
	// Redact before truncation so a credential cannot be cut into an
	// unrecognizable prefix or suffix. JSON quoting prevents fence injection.
	text = tail(r.redact(text), 8000)
	raw, _ := json.Marshal(text)
	return string(raw)
}

func (r *runner) comment(ctx context.Context, body string) error {
	return r.gh(ctx, nil, "pr", "comment", strconv.Itoa(r.payload.PR), "--repo", r.cfg.Repo, "--body", r.redact(body))
}

func (r *runner) block(ctx context.Context, reason string) error {
	// A failed explanatory comment must not leave an exited worker marked
	// running. Attempt the terminal report independently, retaining both errors.
	noteErr := r.note(ctx, "@"+r.cfg.Owner+" "+reason)
	return errors.Join(noteErr, r.outcome(ctx, "blocked"))
}
