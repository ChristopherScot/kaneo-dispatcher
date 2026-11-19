package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

type claudeAccount struct{ name, token string }
type claudeResult struct {
	Type      string  `json:"type"`
	IsError   *bool   `json:"is_error"`
	Result    *string `json:"result"`
	Subtype   string  `json:"subtype"`
	APIError  string  `json:"api_error"`
	APIStatus int     `json:"api_error_status"`
}

func decodeClaude(raw string) (claudeResult, error) {
	var result claudeResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		// CLI diagnostics can precede its final single-line JSON result.
		trimmed := strings.TrimSpace(raw)
		newline := strings.LastIndexByte(trimmed, '\n')
		if newline < 0 {
			return result, fmt.Errorf("invalid Claude result: %w", err)
		}
		result = claudeResult{}
		if err = json.Unmarshal([]byte(trimmed[newline+1:]), &result); err != nil {
			return result, fmt.Errorf("invalid Claude result: %w", err)
		}
	}
	if result.Type != "result" || result.IsError == nil || result.Result == nil {
		return result, fmt.Errorf("incomplete Claude result")
	}
	return result, nil
}

func (result claudeResult) quota() bool {
	if result.IsError == nil || !*result.IsError {
		return false
	}
	switch result.APIError {
	case "usage_limit_reached", "model_requires_usage_credits", "long_context_credits_required":
		return true
	}
	return false
}
func (result claudeResult) unavailableModel() bool {
	if result.IsError == nil || !*result.IsError {
		return false
	}
	text := ""
	if result.Result != nil {
		text = strings.ToLower(*result.Result)
	}
	return result.APIStatus == 404 || strings.Contains(text, "not support this model") || strings.Contains(text, "model is unavailable")
}

func (r *runner) claudeAccounts() ([]claudeAccount, error) {
	var accounts []claudeAccount
	for _, account := range []struct{ name, file string }{{"work", "oauth-token"}, {"personal", "oauth-token-personal"}} {
		raw, err := os.ReadFile(filepath.Join(r.cfg.ClaudeTokenDir, account.file))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read %s Claude credential: %w", account.name, err)
		}
		if token := strings.TrimSpace(string(raw)); token != "" {
			accounts = append(accounts, claudeAccount{account.name, token})
		}
	}
	if len(accounts) > 0 {
		return accounts, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(filepath.Join(home, ".claude", ".credentials.json"))
	if err != nil || info.Size() == 0 {
		return nil, fmt.Errorf("no mounted Claude credentials or CLI login")
	}
	return []claudeAccount{{name: "login"}}, nil
}

// The budget covers the probe and all account attempts. Every retry receives
// the same immutable prompt; only confirmed account quota permits a replay.
func (r *runner) claude(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.payload.TimeoutSeconds)*time.Second)
	defer cancel()
	accounts, err := r.claudeAccounts()
	if err != nil {
		return "", err
	}
	redactionAccounts := slices.Clone(accounts)
	slices.SortFunc(redactionAccounts, func(a, b claudeAccount) int { return len(b.token) - len(a.token) })
	redact := func(s string) string {
		s = r.redact(s)
		for _, account := range redactionAccounts {
			if account.token != "" {
				s = strings.ReplaceAll(s, account.token, "[redacted]")
			}
		}
		return oauthSecret.ReplaceAllString(s, "[redacted]")
	}
	model := r.cfg.Model
	probeCtx, probeCancel := context.WithTimeout(ctx, 120*time.Second)
	probe, err := r.claudeAttempt(probeCtx, accounts[0], model, "probe", "Reply with exactly: OK", redact)
	probeCancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return "", err
		}
		switch {
		case (probe.quota() || probe.unavailableModel()) && r.cfg.Fallback != "" && r.cfg.Fallback != model:
			model = r.cfg.Fallback
			r.log.Printf("Claude model unavailable for this account; using %s", model)
		case probe.quota() && len(accounts) > 1:
			accounts = accounts[1:]
			r.log.Print("Claude probe confirmed work account quota; using personal account")
		default:
			return "", err
		}
	}
	for i, account := range accounts {
		result, err := r.claudeAttempt(ctx, account, model, account.name, r.prompt, redact)
		text := ""
		if result.Result != nil {
			text = redact(*result.Result)
		}
		if err == nil {
			return text, nil
		}
		if ctx.Err() != nil {
			return text, ctx.Err()
		}
		if !result.quota() || i+1 == len(accounts) {
			return text, err
		}
		r.log.Print("Claude account quota exhausted; retrying frozen prompt on personal account")
	}
	return "", fmt.Errorf("no Claude account attempted")
}

var oauthSecret = regexp.MustCompile(`sk-ant-[A-Za-z0-9_-]+`)

func (r *runner) claudeAttempt(ctx context.Context, account claudeAccount, model, attempt, input string, redact func(string) string) (claudeResult, error) {
	prefix := filepath.Join(r.archive, "claude-"+attempt)
	metadata, err := json.Marshal(struct {
		Account, Model string
		StartedAt      time.Time
	}{account.name, model, time.Now().UTC()})
	if err != nil {
		return claudeResult{}, err
	}
	if err = writeNew(prefix+".request.json", metadata); err != nil {
		return claudeResult{}, err
	}
	r.log.Printf("Claude %s: account=%s model=%s", attempt, account.name, model)
	args := []string{"-p", "--output-format", "json", "--model", model, "--permission-mode", "acceptEdits", "--settings", r.cfg.Settings, "--add-dir", r.dir, "--add-dir", r.archive}
	// Headless Claude waits for background jobs even after its final answer.
	// Keep commanded work in the foreground: tool timeouts must stop the tool,
	// not silently start a background build that outlives the stage's verdict.
	env := toolEnv(account.token, r.ghToken)
	env = append(env, "CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1")
	raw, processErr := process(ctx, r.lock, r.dir, input, env, "claude", args...)
	errorText := ""
	if processErr != nil {
		errorText = redact(processErr.Error())
	}
	output, err := json.Marshal(struct{ Output, Error string }{redact(raw), errorText})
	if err != nil {
		return claudeResult{}, err
	}
	if err = writeNew(prefix+".result.json", output); err != nil {
		return claudeResult{}, err
	}
	result, decodeErr := decodeClaude(raw)
	if decodeErr != nil {
		result = claudeResult{}
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if processErr != nil {
		// A signal, output overflow, or failed cleanup is not a quota retry, even
		// if a partial result happened to contain an earlier quota response.
		var exit *exec.ExitError
		if !errors.As(processErr, &exit) || exit.ExitCode() < 0 {
			return claudeResult{}, fmt.Errorf("Claude process: %s", errorText)
		}
		return result, fmt.Errorf("Claude process: %s", errorText)
	}
	if decodeErr != nil {
		return claudeResult{}, decodeErr
	}
	if *result.IsError || (result.Subtype != "" && result.Subtype != "success") {
		return result, fmt.Errorf("Claude reported failure: %s", redact(*result.Result))
	}
	return result, nil
}
