package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewOwnerRepoWorksInEveryStage(t *testing.T) {
	for column, st := range routes {
		t.Run(column, func(t *testing.T) {
			f := newFixture(t)
			f.enqueue("new-task", column, "medium", time.Now().Add(-time.Minute))
			f.kaneo.labels["new-task"] = []string{"repo:brand-new-project"}
			if column != authorColumn {
				f.github.prs["TestOwner:fix/new-task"][0]["head"].(map[string]any)["repo"] = map[string]string{"full_name": "TestOwner/brand-new-project"}
			}
			f.signed("/", move("new-task", column))
			launches := f.pods.launches()
			if len(launches) != 1 || launches[0].pod != st.pod() || launches[0].argv[5] != "TestOwner/brand-new-project" {
				t.Fatalf("new repo not dispatched: %v; %s", launches, f.log.String())
			}
		})
	}
}

func TestOutsideOwnerCannotIssueCommand(t *testing.T) {
	f := newFixture(t)
	if _, err := f.d.issueCommand(t.Context(), "task", "Other/repo", authorColumn, routes[authorColumn], workState{}); err == nil {
		t.Fatal("issued outside-owner command")
	}
	if len(f.github.calls()) != 0 || f.kaneo.calls() != 0 {
		t.Fatal("outside-owner command performed remote reads")
	}
}

func TestStoredOutsideOwnerCommandCannotStartOrAuthorizeWrites(t *testing.T) {
	f := newFixture(t)
	f.stale(authorColumn)
	command := commandFor(t, f, authorColumn)
	cm, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command.Repo = "Other/repo"
	if err := f.d.saveCommandRecord(t.Context(), cm, "t1", command); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"version": 1, "task": "t1", "command": command.ID})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/worker-command", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer testkey")
	if result := f.serve(req); result.Code != http.StatusConflict {
		t.Fatalf("outside-owner prompt: %d", result.Code)
	}
	if code := reportTo(t, f, command, "author-started"); code != http.StatusConflict || f.kaneo.puts != 0 {
		t.Fatalf("outside-owner start: code=%d writes=%d", code, f.kaneo.puts)
	}
	for _, event := range []string{"merge-authorize", "merge-authorize-revert"} {
		if err := f.d.authorizeGitHub(t.Context(), cm, workerReport{Event: event}, command); err == nil {
			t.Fatalf("outside-owner %s accepted", event)
		}
	}
}

func TestTransferredRepositoryCannotStartUnderOldLabel(t *testing.T) {
	f := newFixture(t)
	f.stale(authorColumn)
	command := commandFor(t, f, authorColumn)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(repositoryInfo{FullName: "Other/testrepo", DefaultBranch: "main"})
	}))
	t.Cleanup(srv.Close)
	f.d.githubURL = srv.URL
	if _, err := f.d.ownedRepository(t.Context(), command.Repo); err == nil {
		t.Fatal("transferred repository accepted")
	}
	if code := reportTo(t, f, command, "author-started"); code != http.StatusConflict || f.kaneo.puts != 0 {
		t.Fatalf("transferred repository started: %d, writes=%d", code, f.kaneo.puts)
	}
}
