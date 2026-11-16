package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

func (r *runner) git(ctx context.Context, args ...string) (string, error) {
	out, err := r.tool(ctx, "git", args...)
	return strings.TrimSpace(out), err
}
func (r *runner) checkout(ctx context.Context) error {
	p := r.payload
	for _, branch := range []string{p.Base, p.Branch} {
		if _, err := r.git(ctx, "check-ref-format", "--branch", branch); err != nil {
			return err
		}
	}
	dirty, err := r.git(ctx, "status", "--porcelain")
	if err != nil {
		return err
	}
	if dirty != "" {
		return fmt.Errorf("local edits preserved; checkout held")
	}
	for _, ref := range []string{"HEAD", "refs/heads/" + p.Branch} {
		if ref != "HEAD" {
			if _, err := r.git(ctx, "show-ref", "--verify", "--quiet", ref); err != nil {
				var exit *exec.ExitError
				if errors.As(err, &exit) && exit.ExitCode() == 1 {
					continue
				}
				return err
			}
		}
		count, err := r.git(ctx, "rev-list", "--count", ref, "--not", "--remotes=origin")
		if err != nil {
			return err
		}
		if count != "0" {
			return fmt.Errorf("unpushed commits preserved; checkout held")
		}
	}

	if _, err = r.git(ctx, "fetch", "--quiet", "--no-prune", "origin"); err != nil {
		return err
	}
	refs, err := r.git(ctx, "ls-remote", "origin", "refs/heads/"+p.Base, "refs/heads/"+p.Branch)
	if err != nil {
		return err
	}
	found := map[string]string{}
	for _, line := range strings.Split(refs, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			found[fields[1]] = fields[0]
		}
	}
	if found["refs/heads/"+p.Base] != p.BaseSHA || found["refs/heads/"+p.Branch] != p.BranchSHA {
		return fmt.Errorf("command revisions changed; await a new command")
	}
	sha := p.BranchSHA
	if p.Mode == "author" {
		sha = p.BaseSHA
	}
	if sha == "" {
		return fmt.Errorf("missing checkout revision")
	}
	if _, err = r.git(ctx, "checkout", "--quiet", "-B", p.Branch, sha); err != nil {
		return err
	}
	if p.Mode != "author" {
		_, err = r.git(ctx, "branch", "--set-upstream-to=origin/"+p.Branch, p.Branch)
	}
	return err
}
func (r *runner) verifyHead(ctx context.Context) error {
	head, err := r.git(ctx, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if head != r.payload.HeadSHA {
		return fmt.Errorf("worker checkout changed; refusing verdict")
	}
	return nil
}
func (r *runner) gh(ctx context.Context, out any, args ...string) error {
	raw, err := r.tool(ctx, "gh", args...)
	if err != nil {
		return err
	}
	if out != nil {
		return json.Unmarshal([]byte(raw), out)
	}
	return nil
}
func (r *runner) checks(ctx context.Context) (string, error) {
	// Read checks by the frozen SHA. A PR's current head may have moved.
	var runs struct {
		Total     int                                   `json:"total_count"`
		CheckRuns []struct{ Status, Conclusion string } `json:"check_runs"`
	}
	if err := r.gh(ctx, &runs, "api", "repos/"+r.cfg.Repo+"/commits/"+r.payload.HeadSHA+"/check-runs?per_page=100"); err != nil {
		return "", err
	}
	if runs.Total > len(runs.CheckRuns) {
		return "unknown", nil
	}
	var statuses struct {
		Total    int `json:"total_count"`
		Statuses []struct{ State, Context string }
	}
	if err := r.gh(ctx, &statuses, "api", "repos/"+r.cfg.Repo+"/commits/"+r.payload.HeadSHA+"/status?per_page=100"); err != nil {
		return "", err
	}
	if statuses.Total > len(statuses.Statuses) {
		return "unknown", nil
	}
	pending, success := false, false
	for _, c := range runs.CheckRuns {
		if c.Status != "completed" {
			pending = true
			continue
		}
		switch c.Conclusion {
		case "failure", "timed_out", "cancelled", "action_required", "startup_failure":
			return "failure", nil
		case "success":
			success = true
		case "skipped", "neutral":
		default:
			pending = true
		}
	}
	for _, s := range statuses.Statuses {
		if s.Context == "nightly/integration" {
			continue
		}
		switch s.State {
		case "failure", "error":
			return "failure", nil
		case "success":
			success = true
		default:
			pending = true
		}
	}
	if pending {
		return "pending", nil
	}
	if success {
		return "success", nil
	}
	return "unknown", nil
}
