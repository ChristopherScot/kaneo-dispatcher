package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

//go:embed prompts/*.md
var promptTemplates embed.FS

const maxPromptBytes = 64 << 10
const commandStoreBudget = 768 << 10     // new snapshots
const commandMetadataBudget = 1000 << 10 // progress can use reserved headroom

// Payload is a JSON string so its exact hashed bytes survive HTTP/JSON and
// ConfigMap round trips. Never hash a client's reserialization of this JSON.
type commandPrompt struct {
	Template       string `json:"template"`
	TemplateDigest string `json:"templateDigest"`
	Digest         string `json:"digest"`
	Payload        string `json:"payload,omitempty"`
}

type promptTask struct {
	ID          string          `json:"id"`
	Project     string          `json:"projectId"`
	Title       string          `json:"title"`
	Description json.RawMessage `json:"description"`
}
type promptComment struct {
	Type      string `json:"type"`
	Content   string `json:"content"`
	CreatedAt string `json:"createdAt"`
}

type promptPayload struct {
	Version        int             `json:"version"`
	Task           promptTask      `json:"task"`
	Repo           string          `json:"repo"`
	Stage          string          `json:"stage"`
	Mode           string          `json:"mode"`
	Branch         string          `json:"branch"`
	Base           string          `json:"base"`
	BaseSHA        string          `json:"baseSHA"`
	BranchSHA      string          `json:"branchSHA"`
	PR             int             `json:"pr"`
	HeadSHA        string          `json:"headSHA"`
	Reworks        int             `json:"reworks"`
	TimeoutSeconds int             `json:"timeoutSeconds"`
	Result         string          `json:"result"`
	Instructions   string          `json:"instructions"`
	Comments       []promptComment `json:"comments"`
}

func digest(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func (d *dispatcher) snapshotPrompt(ctx context.Context, id, repo, stage string) (*commandPrompt, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	payload := promptPayload{Version: 1, Repo: repo, Stage: stage, Branch: d.branchPrefix + id, Comments: []promptComment{}}
	if err := d.kaneo(ctx, "/task/"+url.PathEscape(id), &payload.Task); err != nil {
		return nil, err
	}
	if payload.Task.ID != id || payload.Task.Project != d.project {
		return nil, errCardChanged
	}
	var activities []promptComment
	if err := d.kaneo(ctx, "/activity/"+url.PathEscape(id), &activities); err != nil {
		return nil, err
	}
	if activities == nil {
		return nil, fmt.Errorf("task activity is not an array")
	}
	for _, activity := range activities {
		if activity.Type == "comment" {
			payload.Comments = append(payload.Comments, activity)
			if strings.HasPrefix(activity.Content, "Sent back for rework") {
				payload.Reworks++
			}
		}
	}
	var repository struct {
		DefaultBranch string `json:"default_branch"`
	}
	if err := d.github(ctx, "/repos/"+repo, &repository); err != nil {
		return nil, err
	}
	if repository.DefaultBranch == "" {
		return nil, fmt.Errorf("repository has no default branch")
	}
	payload.Base = repository.DefaultBranch
	var err error
	payload.BaseSHA, err = d.branchSHA(ctx, repo, payload.Base)
	if err != nil {
		return nil, err
	}
	payload.BranchSHA, err = d.branchSHA(ctx, repo, payload.Branch)
	var status *httpStatusError
	if errors.As(err, &status) && status.Code == http.StatusNotFound {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	pr, err := d.taskPullRequest(ctx, repo, id)
	if err != nil {
		return nil, err
	}
	// Closed/merged historical PRs are not revision targets.
	if pr != nil && pr.MergedAt == nil && pr.State == "open" {
		payload.PR, payload.HeadSHA = pr.Number, pr.Head.SHA
		if payload.HeadSHA == "" || payload.HeadSHA != payload.BranchSHA {
			return nil, blocked("PR branch changed while constructing command")
		}
	}
	mode := stage
	ahead := 0
	switch stage {
	case "author":
		payload.TimeoutSeconds, payload.Result = 3600, "author-v1"
		if payload.PR > 0 {
			switch {
			case payload.Reworks == 0:
				mode = "advance"
			case payload.Reworks >= 2:
				mode = "blocked"
			default:
				mode = "rework"
			}
		} else if payload.BranchSHA != "" {
			var comparison struct {
				Ahead *int `json:"ahead_by"`
			}
			if err := d.github(ctx, "/repos/"+repo+"/compare/"+payload.BaseSHA+"..."+payload.BranchSHA, &comparison); err != nil {
				return nil, err
			}
			if comparison.Ahead == nil || *comparison.Ahead < 0 {
				return nil, fmt.Errorf("invalid branch comparison")
			}
			ahead = *comparison.Ahead
			if ahead > 0 {
				mode = "resume"
			}
		}
	case "test", "merge":
		if payload.PR == 0 {
			return nil, blocked("no open same-repository PR for %s", id)
		}
		payload.TimeoutSeconds = 2700
		payload.Result = "test-verdict-v1"
		if stage == "merge" {
			mode, payload.Result = "review", "merge-verdict-v1"
			payload.TimeoutSeconds = 1800
		}
	default:
		return nil, fmt.Errorf("unknown stage %q", stage)
	}
	payload.Mode = mode
	instructions := "Do not invoke Claude for this command. Report the dispatcher-selected author outcome."
	if mode != "advance" && mode != "blocked" {
		raw, err := promptTemplates.ReadFile("prompts/" + mode + ".md")
		if err != nil {
			return nil, err
		}
		instructions = string(raw)
	}
	templateHash := digest(instructions)
	// Only template text is substituted. Ticket/comment text is appended by the
	// worker as JSON data and is never passed through a template engine.
	payload.Instructions = strings.NewReplacer(
		"{{task}}", id, "{{repo}}", repo, "{{branch}}", payload.Branch,
		"{{head_sha}}", payload.HeadSHA, "{{pr}}", fmt.Sprint(payload.PR), "{{resume_ahead}}", fmt.Sprint(ahead),
		"{{board_url}}", envOr("KANEO_WEB_URL", "https://kaneo.home.chrisscotmartin.com"),
	).Replace(instructions)
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxPromptBytes {
		return nil, blocked("prompt and ticket context exceed %d bytes", maxPromptBytes)
	}
	return &commandPrompt{Template: mode, TemplateDigest: templateHash, Digest: digest(string(raw)), Payload: string(raw)}, nil
}

func (d *dispatcher) branchSHA(ctx context.Context, repo, ref string) (string, error) {
	var reference struct {
		Ref    string `json:"ref"`
		Object struct {
			SHA  string `json:"sha"`
			Type string `json:"type"`
		} `json:"object"`
	}
	if err := d.github(ctx, "/repos/"+repo+"/git/ref/heads/"+url.PathEscape(ref), &reference); err != nil {
		return "", err
	}
	if reference.Ref != "refs/heads/"+ref || reference.Object.Type != "commit" || reference.Object.SHA == "" {
		return "", fmt.Errorf("invalid branch reference for %s", ref)
	}
	return reference.Object.SHA, nil
}

// A fetch does not start work or change the command. The worker saves the
// exact payload locally before asking the existing endpoint to start it.
func (d *dispatcher) workerPrompt(w http.ResponseWriter, r *http.Request) {
	if !d.authorizedWorker(w, r) {
		return
	}
	var request struct {
		Task    string `json:"task"`
		Command string `json:"command"`
		Version int    `json:"version"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.Version != 1 || request.Task == "" || request.Command == "" {
		http.Error(w, "invalid prompt request", 400)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		http.Error(w, "expected one request", 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	cm, err := d.commandStore(ctx)
	if err != nil {
		http.Error(w, "command store unavailable", 503)
		return
	}
	command, err := readCommand(cm, request.Task)
	if err != nil || command == nil || command.ID != request.Command || command.Prompt == nil || command.Prompt.Payload == "" || (command.Phase != "issued" && command.Phase != "running") {
		http.Error(w, "command has no available prompt", 409)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "task": request.Task, "command": command.ID, "repo": command.Repo, "stage": command.Stage, "prompt": command.Prompt})
}

func (d *dispatcher) validatePromptStart(ctx context.Context, command *workerCommand, suppliedDigest string) error {
	if command.Prompt == nil {
		return nil
	} // pre-upgrade command already in flight
	if command.Prompt.Payload == "" || suppliedDigest != command.Prompt.Digest || digest(command.Prompt.Payload) != command.Prompt.Digest {
		return blocked("worker has not acknowledged the command's prompt snapshot")
	}
	var payload promptPayload
	if err := json.Unmarshal([]byte(command.Prompt.Payload), &payload); err != nil {
		return err
	}
	base, err := d.branchSHA(ctx, command.Repo, payload.Base)
	if err != nil {
		return err
	}
	branch, err := d.branchSHA(ctx, command.Repo, payload.Branch)
	var status *httpStatusError
	if errors.As(err, &status) && status.Code == http.StatusNotFound {
		err = nil
	}
	if err != nil {
		return err
	}
	if base != payload.BaseSHA || branch != payload.BranchSHA {
		return blocked("prompt's repository revision changed; await a replacement command")
	}
	pr, err := d.taskPullRequest(ctx, command.Repo, payload.Task.ID)
	if err != nil {
		return err
	}
	if payload.PR > 0 {
		if pr == nil || pr.Number != payload.PR || pr.State != "open" || pr.MergedAt != nil || pr.Head.SHA != payload.HeadSHA {
			return blocked("prompt's PR changed; await a replacement command")
		}
	} else if pr != nil && pr.State == "open" {
		return blocked("a PR appeared after this prompt was selected")
	}
	return nil
}
