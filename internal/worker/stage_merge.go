package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/protocol"
)

func (r *runner) merge(ctx context.Context) error {
	var status struct {
		Statuses []struct{ Context, State string }
	}
	if err := r.gh(ctx, &status, "api", "repos/"+r.cfg.Repo+"/commits/"+r.payload.HeadSHA+"/status"); err != nil {
		return err
	}
	tested := false
	for _, s := range status.Statuses {
		if s.Context == "nightly/integration" {
			tested = s.State == "success"
			break
		}
	}
	if !tested {
		return fmt.Errorf("no successful integration verdict for commanded head; merge held")
	}
	state, err := r.checks(ctx)
	if err != nil {
		return err
	}
	if state != "success" {
		return fmt.Errorf("CI is %s; merge held", state)
	}
	text, err := r.claude(ctx)
	if err != nil {
		// A cancelled review deliberately reports nothing: the dispatcher
		// reissues the merge after its stale timeout, with no human needed.
		return err
	}
	if err := r.verifyHead(ctx); err != nil {
		return errors.Join(err, r.block(ctx, "The review checkout changed; its verdict was refused."))
	}
	decision, err := mergeVerdict(text)
	if err != nil {
		return err
	}
	if err := r.comment(ctx, "Nightly review (independent panel): "+decision+"\n\n"+r.evidence(text)); err != nil {
		return err
	}
	switch decision {
	case "RISKY":
		if err := r.gh(ctx, nil, "label", "create", "risky", "--repo", r.cfg.Repo, "--color", "B60205", "--description", "Review requires a human decision", "--force"); err != nil {
			r.log.Print("risk label: ", r.redact(err.Error()))
		}
		if err := r.gh(ctx, nil, "pr", "edit", strconv.Itoa(r.payload.PR), "--repo", r.cfg.Repo, "--add-label", "risky"); err != nil {
			r.log.Print("risk label: ", r.redact(err.Error()))
		}
		return r.block(ctx, "The review panel flagged this pull request's blast radius for a human decision.")
	case "NO-MERGE":
		return r.rework(ctx, "The review panel declined the pull request.\n\n"+r.evidence(text))
	}
	method, err := r.mergeMethod(ctx)
	if err != nil {
		return err
	}
	if err := r.report(ctx, protocol.Report{Event: "merge-authorize", PR: r.payload.PR, SHA: r.payload.HeadSHA}); err != nil {
		return err
	}
	merged, err := r.mergePR(ctx, r.payload.PR, r.payload.HeadSHA, method, "merge")
	if err != nil {
		return err
	}
	return r.verifyMerge(ctx, merged, method)
}

// Only the final nonempty line can be a verdict. Earlier prose and strings
// such as "MERGEABLE" cannot authorize a merge.
func mergeVerdict(text string) (string, error) {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	last := strings.TrimSpace(lines[len(lines)-1])
	fields := strings.Fields(last)
	if len(fields) >= 2 && fields[0] == "VERDICT:" && (len(fields) == 2 || len(fields) >= 4 && fields[2] == "-") {
		switch fields[1] {
		case "MERGE", "NO-MERGE", "RISKY":
			return fields[1], nil
		}
	}
	return "", fmt.Errorf("no valid final review verdict; merge held")
}

type mergeReceipt struct {
	Repo     string `json:"repo"`
	PR       int    `json:"pr"`
	HeadSHA  string `json:"headSHA"`
	MergeSHA string `json:"mergeSHA,omitempty"`
}

func (r *runner) record(name string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeNew(filepath.Join(r.archive, name+".json"), raw)
}

func (r *runner) mergeMethod(ctx context.Context) (string, error) {
	var repo struct {
		Squash bool `json:"allow_squash_merge"`
		Merge  bool `json:"allow_merge_commit"`
		Rebase bool `json:"allow_rebase_merge"`
	}
	if err := r.gh(ctx, &repo, "api", "repos/"+r.cfg.Repo); err != nil {
		return "", err
	}
	switch {
	case repo.Squash:
		return "squash", nil
	case repo.Merge:
		return "merge", nil
	case repo.Rebase:
		return "rebase", nil
	default:
		return "", fmt.Errorf("repository has no enabled merge method")
	}
}

func (r *runner) mergePR(ctx context.Context, pr int, sha, method, name string) (mergeReceipt, error) {
	receipt := mergeReceipt{Repo: r.cfg.Repo, PR: pr, HeadSHA: sha}
	if pr <= 0 || sha == "" {
		return receipt, fmt.Errorf("missing pinned merge identity")
	}
	if err := r.record(name+"-intent", receipt); err != nil {
		return receipt, err
	}
	var result struct {
		Merged bool
		SHA    string
	}
	if err := r.gh(ctx, &result, "api", "--method", "PUT", fmt.Sprintf("repos/%s/pulls/%d/merge", r.cfg.Repo, pr), "-f", "sha="+sha, "-f", "merge_method="+method); err != nil {
		return receipt, fmt.Errorf("merge outcome uncertain; inspect durable intent: %w", err)
	}
	if !result.Merged || result.SHA == "" {
		return receipt, fmt.Errorf("GitHub did not confirm a merge; command held")
	}
	receipt.MergeSHA = result.SHA
	if err := r.record(name+"-completed", receipt); err != nil {
		return receipt, fmt.Errorf("persist confirmed merge: %w", err)
	}
	return receipt, nil
}

type workflowRun struct{ Status, Conclusion string }

func workflowResult(runs []workflowRun) string {
	if len(runs) == 0 {
		return "unknown"
	}
	success, pending, failed := false, false, false
	for _, run := range runs {
		if run.Status != "completed" {
			pending = true
			continue
		}
		switch run.Conclusion {
		case "success":
			success = true
		case "skipped", "neutral":
		case "failure", "timed_out":
			failed = true
		default:
			return "unknown"
		}
	}
	if pending {
		return "pending"
	}
	if failed {
		return "failure"
	}
	if success {
		return "success"
	}
	return "unknown"
}

func (r *runner) verifyMerge(ctx context.Context, receipt mergeReceipt, method string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	for attempt := 0; attempt < 40; attempt++ {
		var runs []workflowRun
		if err := r.gh(waitCtx, &runs, "run", "list", "--repo", r.cfg.Repo, "--branch", r.payload.Base, "--commit", receipt.MergeSHA, "--limit", "100", "--json", "status,conclusion"); err != nil {
			return err
		}
		// A full page may hide unfinished or failing runs; never guess green.
		state := "unknown"
		if len(runs) < 100 {
			state = workflowResult(runs)
		}
		switch state {
		case "success":
			return r.outcome(ctx, "succeeded")
		case "failure":
			return r.revertMerge(ctx, receipt, method)
		}
		if err := waitStage(waitCtx, 30*time.Second); err != nil {
			return err
		}
	}
	return fmt.Errorf("exact merge-commit CI not confirmed; command held")
}

func waitStage(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *runner) revertMerge(ctx context.Context, receipt mergeReceipt, method string) error {
	if err := r.report(ctx, protocol.Report{Event: "merge-authorize-revert", PR: receipt.PR, SHA: receipt.HeadSHA}); err != nil {
		return err
	}
	if err := r.record("revert-intent", receipt); err != nil {
		return err
	}
	output, err := r.tool(ctx, "gh", "pr", "revert", strconv.Itoa(receipt.PR), "--repo", r.cfg.Repo, "--body", fmt.Sprintf("Reverts #%d. Opened by stage-merge because the exact merge commit failed CI.", receipt.PR))
	if err != nil {
		return fmt.Errorf("revert creation uncertain; inspect durable intent: %w", err)
	}
	url := strings.TrimSpace(output)
	prefix := "https://github.com/" + r.cfg.Repo + "/pull/"
	number := 0
	if strings.HasPrefix(url, prefix) {
		number, err = strconv.Atoi(strings.TrimPrefix(url, prefix))
	}
	if err != nil || number <= 0 {
		return fmt.Errorf("unrecognized revert PR response; inspect durable intent")
	}
	if err := r.record("revert-created", struct {
		Number int
		URL    string
	}{number, url}); err != nil {
		return err
	}
	for attempt := 0; attempt < 6; attempt++ {
		var pr struct {
			Number    int
			Merged    bool
			Mergeable *bool
			Head      struct{ SHA string }
			Base      struct {
				Ref  string
				Repo struct {
					FullName string `json:"full_name"`
				}
			}
		}
		if err := r.gh(ctx, &pr, "api", fmt.Sprintf("repos/%s/pulls/%d", r.cfg.Repo, number)); err != nil {
			return err
		}
		if pr.Number != number || pr.Merged || pr.Head.SHA == "" || pr.Base.Ref != r.payload.Base || !strings.EqualFold(pr.Base.Repo.FullName, r.cfg.Repo) {
			return fmt.Errorf("revert PR identity changed; command held")
		}
		if pr.Mergeable != nil {
			if !*pr.Mergeable {
				return fmt.Errorf("revert PR cannot merge; manual recovery required: %s", url)
			}
			if _, err := r.mergePR(ctx, number, pr.Head.SHA, method, "revert-merge"); err != nil {
				return err
			}
			if err := r.note(ctx, "The merge failed CI and was reverted: "+url); err != nil {
				return err
			}
			if err := r.report(ctx, protocol.Report{Event: "merge-reverted", RevertPR: number}); err != nil {
				return err
			}
			return r.comment(ctx, "The exact merge commit failed CI and was reverted: "+url)
		}
		if err := waitStage(ctx, 10*time.Second); err != nil {
			return err
		}
	}
	return fmt.Errorf("revert PR mergeability unknown; manual recovery required: %s", url)
}
