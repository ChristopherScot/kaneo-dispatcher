// Package worker executes one frozen dispatcher command. It owns local
// execution only; the dispatcher authorizes all workflow transitions.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/protocol"
)

const DefaultLock = "/tmp/nightly.lock"

type Config struct {
	Stage, Task, Repo, Command                                                 string
	Workspace, StateDir, Lock, LaunchAck, DispatcherURL                        string
	APIKeyFile, GitTokenFile, ClaudeTokenDir, Settings, Model, Fallback, Owner string
}

func Environment(stage, task, repo, command string) Config {
	return Config{Stage: stage, Task: task, Repo: repo, Command: command,
		Workspace: "/workspace", StateDir: env("NIGHTLY_STATE_DIR", "/workspace/.nightly"),
		Lock: env("NIGHTLY_LOCK", DefaultLock), LaunchAck: os.Getenv("NIGHTLY_LAUNCH_ACK"),
		DispatcherURL: env("KANEO_DISPATCH_URL", "http://kaneo-dispatcher.claude-pods.svc.cluster.local:8080"),
		APIKeyFile:    "/run/secrets/kaneo/api-key", GitTokenFile: "/run/secrets/git/token",
		ClaudeTokenDir: env("CLAUDE_TOKEN_DIR", "/run/secrets/claude"),
		Settings:       env("NIGHTLY_SETTINGS", "/usr/local/share/claude-pods/prompts/nightly-settings.json"),
		Model:          env("NIGHTLY_MODEL", "claude-fable-5-1"), Fallback: env("NIGHTLY_MODEL_FALLBACK", "opus"), Owner: env("KANEO_OWNER", "chris")}
}
func env(name, fallback string) string {
	if s := os.Getenv(name); s != "" {
		return s
	}
	return fallback
}

type runner struct {
	lock                                       *os.File
	cfg                                        Config
	http                                       *http.Client
	key, ghToken, dir, archive, prompt, digest string
	payload                                    protocol.Payload
	log                                        *log.Logger
}

func Run(ctx context.Context, cfg Config) error {
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`).MatchString(cfg.Command) || cfg.Task == "" ||
		!regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`).MatchString(cfg.Repo) ||
		(cfg.Stage != "author" && cfg.Stage != "test" && cfg.Stage != "merge") {
		return fmt.Errorf("stage, task, repository and command are required")
	}
	if filepath.Base(cfg.Repo) == "." || filepath.Base(cfg.Repo) == ".." {
		return fmt.Errorf("invalid repository")
	}
	key, err := secret(cfg.APIKeyFile)
	if err != nil {
		return err
	}
	gh, err := secret(cfg.GitTokenFile)
	if err != nil {
		return err
	}
	lock, err := acquireLock(cfg.Lock, cfg.LaunchAck)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = makeDurableDirs(cfg.StateDir); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(cfg.StateDir, cfg.Stage+"-"+cfg.Command+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	writers := []io.Writer{os.Stdout, file}
	if container, err := os.OpenFile("/proc/1/fd/1", os.O_WRONLY, 0); err == nil {
		defer container.Close()
		writers = append(writers, container)
	}
	r := runner{cfg: cfg, lock: lock, http: &http.Client{Timeout: 95 * time.Second}, key: key, ghToken: gh,
		dir: filepath.Join(cfg.Workspace, filepath.Base(cfg.Repo)), archive: filepath.Join(cfg.StateDir, "commands", cfg.Command), log: log.New(io.MultiWriter(writers...), "", log.LstdFlags)}
	r.maintenance("warn")
	defer r.maintenance("prune")
	err = r.execute(ctx)
	if err != nil {
		r.log.Print(r.redact(err.Error()))
	}
	if err != nil {
		return fmt.Errorf("%s", r.redact(err.Error()))
	}
	return nil
}
func secret(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read credential %s: %w", path, err)
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", fmt.Errorf("empty credential %s", path)
	}
	return value, nil
}
func (r *runner) redact(s string) string {
	for _, key := range []string{r.ghToken, r.key} {
		if key != "" {
			s = strings.ReplaceAll(s, key, "[redacted]")
		}
	}
	return s
}
func (r *runner) execute(ctx context.Context) error {
	if err := r.fetch(ctx); err != nil {
		return err
	}
	if err := r.report(ctx, protocol.Report{Event: r.cfg.Stage + "-started", PromptDigest: r.digest}); err != nil {
		return err
	}
	switch r.payload.Mode {
	case "advance":
		return r.outcome(ctx, "succeeded")
	case "blocked":
		if err := r.note(ctx, "the review panel rework limit has been reached"); err != nil {
			return err
		}
		return r.outcome(ctx, "blocked")
	}
	if err := r.checkout(ctx); err != nil {
		// Execution was accepted, but no stage work has begun. Preserve the
		// checkout and report a setup block even if execution was cancelled;
		// otherwise the dispatcher mistakes an exited worker for active work.
		reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		return errors.Join(err, r.block(reportCtx, fmt.Sprintf("Worker setup failed for command %s in %s: %s. Existing work was preserved.", r.cfg.Command, r.dir, r.redact(err.Error()))))
	}
	switch r.cfg.Stage {
	case "author":
		return r.author(ctx)
	case "test":
		return r.test(ctx)
	case "merge":
		return r.merge(ctx)
	}
	return fmt.Errorf("unsupported stage")
}
func (r *runner) tool(ctx context.Context, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return process(ctx, r.lock, r.dir, "", toolEnv("", r.ghToken), name, args...)
}
func (r *runner) outcome(ctx context.Context, outcome string) error {
	return r.report(ctx, protocol.Report{Event: r.cfg.Stage + "-" + outcome})
}
func (r *runner) note(ctx context.Context, text string) error {
	return r.report(ctx, protocol.Report{Event: "note", Message: r.redact(text)})
}
func (r *runner) rework(ctx context.Context, why string) error {
	if err := r.report(ctx, protocol.Report{Event: "note", NoteKind: "rework", Message: r.redact(why)}); err != nil {
		return err
	}
	return r.outcome(ctx, "failed")
}

// Housekeeping is best effort and never replaces the command result. All
// supervised children have exited before the deferred prune is invoked.
func (r *runner) maintenance(action string) {
	const script = "/usr/local/share/claude-pods/lib/maintenance.sh"
	if _, err := os.Stat(script); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := process(ctx, r.lock, r.cfg.Workspace, "", toolEnv("", r.ghToken), "bash", script, action)
	if out != "" {
		r.log.Print(r.redact(out))
	}
	if err != nil {
		r.log.Printf("housekeeping: %s", r.redact(err.Error()))
	}
}
