package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func claudeFixture(t *testing.T, script string) (*runner, *bytes.Buffer) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "claude"), []byte("#!/bin/sh\nset -eu\n"+script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	tokens := filepath.Join(dir, "tokens")
	if err := os.Mkdir(tokens, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"oauth-token", "oauth-token-personal"} {
		if err := os.WriteFile(filepath.Join(tokens, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	archive := filepath.Join(dir, "archive")
	if err := os.Mkdir(archive, 0700); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	r := &runner{cfg: Config{ClaudeTokenDir: tokens, Model: "preferred", Fallback: "fallback"}, dir: dir, archive: archive, prompt: "Frozen {{literal}} $INPUT\nSecond line\n", log: log.New(&logs, "", 0)}
	r.payload.TimeoutSeconds = 5
	return r, &logs
}

func TestClaudeReplaysPromptAndArchivesQuotaRetry(t *testing.T) {
	for _, phase := range []string{"attempt", "probe"} {
		t.Run(phase, func(t *testing.T) {
			t.Setenv("QUOTA_PHASE", phase)
			r, logs := claudeFixture(t, `
cat > attempt-input
input=$(cat attempt-input)
if [ "$input" = 'Reply with exactly: OK' ]; then
 if [ "$QUOTA_PHASE" = probe ]; then
  printf '%s' '{"type":"result","is_error":true,"api_error":"usage_limit_reached","result":"quota"}'; exit 1
 fi
 printf '%s' '{"type":"result","is_error":false,"result":"OK"}'; exit
fi
cp attempt-input "$CLAUDE_CODE_OAUTH_TOKEN.prompt"
if [ "$CLAUDE_CODE_OAUTH_TOKEN" = oauth-token ]; then
 printf '%s' '{"type":"result","is_error":true,"api_error_status":429,"api_error":"usage_limit_reached","result":"quota oauth-token"}'; exit 1
fi
printf '%s' '{"type":"result","is_error":false,"result":"finished oauth-token-personal"}'
`)
			if phase == "probe" {
				r.cfg.Fallback = r.cfg.Model
			}
			out, err := r.claude(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if out != "finished [redacted]" {
				t.Fatalf("result=%q", out)
			}
			names := []string{"oauth-token-personal"}
			if phase == "attempt" {
				names = append(names, "oauth-token")
			}
			for _, name := range names {
				input, err := os.ReadFile(filepath.Join(r.dir, name+".prompt"))
				if err != nil {
					t.Fatal(err)
				}
				if string(input) != r.prompt {
					t.Fatalf("%s got changed prompt: %q", name, input)
				}
			}
			archives := []string{"claude-probe.result.json", "claude-personal.result.json"}
			if phase == "attempt" {
				archives = append(archives, "claude-work.result.json")
			}
			for _, name := range archives {
				raw, err := os.ReadFile(filepath.Join(r.archive, name))
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(raw), "oauth-token") {
					t.Fatalf("credential in %s", name)
				}
			}
			if strings.Contains(logs.String(), "oauth-token") {
				t.Fatal("credential in logs")
			}
		})
	}
}

func TestClaudeOneBudgetAcrossProbeAndAccounts(t *testing.T) {
	r, _ := claudeFixture(t, `
input=$(cat)
if [ "$input" = 'Reply with exactly: OK' ]; then
 sleep 0.3
 printf '%s' '{"type":"result","is_error":false,"result":"OK"}'; exit
fi
if [ "$CLAUDE_CODE_OAUTH_TOKEN" = oauth-token ]; then
 sleep 0.3
 printf '%s' '{"type":"result","is_error":true,"api_error":"usage_limit_reached","result":"quota"}'; exit 1
fi
sleep 0.7
printf '%s' '{"type":"result","is_error":false,"result":"late success"}'
`)
	r.payload.TimeoutSeconds = 1
	started := time.Now()
	_, err := r.claude(t.Context())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("claude error=%v, want deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("overall budget took %v", elapsed)
	}
	if _, err := os.Stat(filepath.Join(r.archive, "claude-personal.request.json")); err != nil {
		t.Fatal("personal retry was not exercised", err)
	}
}

func TestClaudeRejectsUnconfirmedQuotaAndErrorResults(t *testing.T) {
	for _, tc := range []struct {
		name, reply string
		exit        int
	}{
		{"capacity", `{"type":"result","is_error":true,"api_error_status":429,"result":"capacity"}`, 0},
		{"error_verdict", `{"type":"result","is_error":true,"result":"VERDICT: MERGE"}`, 0},
		{"incomplete", `{"type":"result","result":"missing error marker"}`, 0},
		{"abnormal_exit", `{"type":"result","is_error":false,"result":"VERDICT: MERGE"}`, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := claudeFixture(t, `input=$(cat)
if [ "$input" = 'Reply with exactly: OK' ]; then
 printf '%s' '{"type":"result","is_error":false,"result":"OK"}'; exit
fi
printf '%s' '`+tc.reply+`'
`+fmt.Sprintf("exit %d\n", tc.exit))
			if _, err := r.claude(t.Context()); err == nil {
				t.Fatal("error result accepted")
			}
			if _, err := os.Stat(filepath.Join(r.archive, "claude-personal.request.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unexpected account retry", err)
			}
		})
	}
}

func TestClaudeLoginAndModelFallback(t *testing.T) {
	r, _ := claudeFixture(t, `
[ -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" ] || exit 10
[ -z "${ANTHROPIC_API_KEY:-}" ] || exit 11
input=$(cat)
if [ "$input" = 'Reply with exactly: OK' ]; then
 printf '%s' '{"type":"result","is_error":true,"api_error_status":404,"result":"model is unavailable"}'; exit 1
fi
case " $* " in *' --model fallback '*) ;; *) exit 12;; esac
printf '%s' '{"type":"result","is_error":false,"result":"login fallback succeeded"}'
`)
	r.cfg.ClaudeTokenDir = filepath.Join(t.TempDir(), "missing")
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("ANTHROPIC_API_KEY", "should-not-be-used")
	if err := os.Mkdir(filepath.Join(home, ".claude"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".claude", ".credentials.json"), []byte(`{"test":"CLI owns validation"}`), 0600); err != nil {
		t.Fatal(err)
	}
	out, err := r.claude(t.Context())
	if err != nil || out != "login fallback succeeded" {
		t.Fatalf("login/model fallback = %q, %v", out, err)
	}
}
