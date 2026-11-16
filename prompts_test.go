package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPromptSnapshotSurvivesChangesAndRejectsStaleStart(t *testing.T) {
	f := newFixture(t)
	f.stale("to-do")
	f.kaneo.tasks["t1"]["description"] = "Keep $KANEO_API_KEY and {{workspace}} literal in ticket data."
	f.kaneo.activity["t1"] = append(f.kaneo.activity["t1"], map[string]string{"type": "comment", "content": "original requirement"})
	command := commandFor(t, f, "to-do")
	original := command.Prompt.Payload
	if digest(original) != command.Prompt.Digest {
		t.Fatal("payload digest mismatch")
	}
	f.kaneo.tasks["t1"]["description"] = "edited later"
	f.kaneo.activity["t1"] = f.kaneo.activity["t1"][:2]
	// Fetch through a new dispatcher object; no template or Kaneo reread occurs.
	replacement := &dispatcher{apiKey: f.d.apiKey, workflows: f.d.workflows}
	request := func(id string, authorized bool) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(map[string]any{"version": 1, "task": "t1", "command": id})
		req := httptest.NewRequest(http.MethodPost, "/worker-command", bytes.NewReader(raw))
		if authorized {
			req.Header.Set("Authorization", "Bearer testkey")
		}
		w := httptest.NewRecorder()
		replacement.mux().ServeHTTP(w, req)
		return w
	}
	if w := request(command.ID, false); w.Code != 401 {
		t.Fatalf("unauthorized fetch: %d", w.Code)
	}
	if w := request("obsolete", true); w.Code != 409 {
		t.Fatalf("wrong command fetch: %d", w.Code)
	}
	w := request(command.ID, true)
	var response struct {
		Prompt commandPrompt `json:"prompt"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Prompt.Payload != original {
		t.Fatal("snapshot changed after issuance/restart")
	}
	// A new PR arriving after selection invalidates a fresh-author command.
	f.github.prs["TestOwner:fix/t1"] = openPR("new-sha")
	err := f.d.acceptReport(t.Context(), workerReport{Task: "t1", Command: command.ID, Event: "author-started", PromptDigest: command.Prompt.Digest})
	if err == nil || !strings.Contains(err.Error(), "revision changed") || f.kaneo.puts != 0 {
		t.Fatal("stale prompt started work")
	}
}

func TestDispatcherSelectsAuthorModeAndBoundsSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name, state, mode string
		reworks           int
	}{
		{"fresh", "", "author", 0}, {"existing unreviewed", "open", "advance", 0},
		{"rework", "open", "rework", 1}, {"capped", "open", "blocked", 2},
		{"resume remote commits", "closed", "resume", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.stale("to-do")
			if tc.state != "" {
				pr := openPR("abc")[0]
				pr["state"] = tc.state
				f.github.prs["TestOwner:fix/t1"] = []map[string]any{pr}
			}
			for range tc.reworks {
				f.kaneo.activity["t1"] = append(f.kaneo.activity["t1"], map[string]string{"type": "comment", "content": "Sent back for rework: fix it"})
			}
			command := commandFor(t, f, "to-do")
			if command.Prompt.Template != tc.mode {
				t.Fatalf("mode=%s want %s", command.Prompt.Template, tc.mode)
			}
		})
	}
	f := newFixture(t)
	f.stale("to-do")
	f.kaneo.tasks["t1"]["description"] = strings.Repeat("x", maxPromptBytes)
	if _, err := f.d.snapshotPrompt(t.Context(), "t1", "TestOwner/testrepo", "author"); err == nil {
		t.Fatal("oversized context accepted")
	}
	cm, err := f.d.commandStore(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cm.Data["other"] = strings.Repeat("x", commandStoreBudget)
	if err := f.d.saveCommandRecord(t.Context(), cm, "t1", &workerCommand{ID: "new", Phase: "issued", Prompt: &commandPrompt{Payload: "snapshot"}}); err == nil {
		t.Fatal("command store budget ignored")
	}
	if err := f.d.saveCommandRecord(t.Context(), cm, "t1", &workerCommand{ID: "existing", Phase: "starting"}); err != nil {
		t.Fatalf("metadata cannot use reserved headroom: %v", err)
	}

}
