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

	"github.com/ChristopherScot/kaneo-dispatcher/internal/protocol"
)

func (r *runner) author(ctx context.Context) error {
	text, runErr := r.claude(ctx)
	// Publish even after an interrupted author run: local commits are useful
	// recovery evidence, but a failed execution is never a successful outcome.
	ahead, err := r.git(ctx, "rev-list", "--count", r.payload.BaseSHA+"..refs/heads/"+r.payload.Branch)
	if err != nil {
		return errors.Join(runErr, err)
	}
	count, err := strconv.Atoi(ahead)
	if err != nil || count < 0 {
		return errors.Join(runErr, fmt.Errorf("invalid authored commit count %q", ahead))
	}
	if count > 0 {
		if _, err := r.git(ctx, "push", "-u", "origin", "refs/heads/"+r.payload.Branch+":refs/heads/"+r.payload.Branch); err != nil {
			return errors.Join(runErr, fmt.Errorf("preserve authored commits: %w", err))
		}
	}
	pr, err := r.authoredPR(ctx)
	if err != nil {
		return errors.Join(runErr, err)
	}
	if pr.Number == 0 {
		if runErr != nil {
			text += "\n" + runErr.Error()
		}
		if err := r.note(ctx, "The author produced no open pull request.\n\n"+r.evidence(text)); err != nil {
			return errors.Join(runErr, err)
		}
		return errors.Join(runErr, r.outcome(ctx, "failed"))
	}
	if err := r.report(ctx, protocol.Report{Event: "link", URL: pr.URL, Title: "open"}); err != nil {
		return errors.Join(runErr, err)
	}
	if runErr != nil {
		return runErr
	}
	return r.outcome(ctx, "succeeded")
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
	if err := r.verifyHead(ctx); err != nil {
		return errors.Join(err, r.block(ctx, "The test checkout changed; its verdict was refused."))
	}
	// checkout required a clean tree, so this removes only this test run's
	// scratch work. Never reset a different commit or the author/reviewer tree.
	defer func() {
		if err := r.verifyHead(ctx); err != nil {
			resultErr = errors.Join(resultErr, err)
			return
		}
		_, err := r.git(ctx, "reset", "--hard", "--quiet", r.payload.HeadSHA)
		if err == nil {
			_, err = r.git(ctx, "clean", "-qfd")
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
		return errors.Join(runErr, r.block(ctx, "Integration testing could not complete on "+r.payload.HeadSHA+"; see the pull request report."))
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
	if err := r.note(ctx, "@"+r.cfg.Owner+" "+reason); err != nil {
		return err
	}
	return r.outcome(ctx, "blocked")
}
