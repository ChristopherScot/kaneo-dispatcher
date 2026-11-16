package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Execute the real launcher: a successful background fork alone must never
// count as dispatch. These fixtures stand in for the worker lock handshake.
func TestLaunchRequiresWorkerAcceptance(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("launcher requires Linux setsid")
	}
	for _, tc := range []struct {
		name, script string
		want         int
	}{
		{"missing stage", "", 1},
		{"early exit", "exit 0", 1},
		{"lock contention", `printf 'busy\n' > "$NIGHTLY_LAUNCH_ACK.tmp"; mv "$NIGHTLY_LAUNCH_ACK.tmp" "$NIGHTLY_LAUNCH_ACK"`, 75},
		{"accepted and finished", `printf 'accepted\n' > "$NIGHTLY_LAUNCH_ACK.tmp"; mv "$NIGHTLY_LAUNCH_ACK.tmp" "$NIGHTLY_LAUNCH_ACK"`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := filepath.Join(t.TempDir(), "stage")
			if tc.script != "" {
				if err := os.WriteFile(script, []byte("#!/bin/sh\n# dispatcher-command-protocol: 3\nset -eu\n"+tc.script+"\n"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.CommandContext(t.Context(), "sh", "-c", launchLine, script, "task", "owner/repo")
			output, err := cmd.CombinedOutput()
			if err != nil && cmd.ProcessState == nil {
				t.Fatal(err)
			}
			if got := cmd.ProcessState.ExitCode(); got != tc.want {
				t.Fatalf("exit=%d want=%d: %s", got, tc.want, output)
			}
		})
	}
}
