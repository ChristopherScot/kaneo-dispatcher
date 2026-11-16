package worker

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestProcessStopsDescendants(t *testing.T) {
	for _, mode := range []string{"exit", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			marker := filepath.Join(dir, "survived")
			script := `(trap '' TERM; sleep 0.6; echo survived > "$1") & echo started`
			ctx := t.Context()
			if mode == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				script += "\nwait"
			}
			out, err := process(ctx, nil, dir, "", os.Environ(), "sh", "-c", script, "child", marker)
			if mode == "cancel" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("process cancellation = %v, want deadline", err)
			}
			if mode == "exit" && err != nil {
				t.Fatal(err)
			}
			if out != "started\n" {
				t.Fatalf("process output = %q", out)
			}
			time.Sleep(700 * time.Millisecond)
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("descendant survived group cleanup: %v", err)
			}
		})
	}
}

func TestProcessBoundsOutput(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	out, err := process(ctx, nil, t.TempDir(), "", os.Environ(), "sh", "-c", "yes output")
	if !errors.Is(err, errOutputLimit) || len(out) > 8<<20 {
		t.Fatalf("output bytes=%d, error=%v", len(out), err)
	}
}

func TestLockAcknowledgmentFencesLaunches(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "worker.lock")
	accepted := filepath.Join(dir, "accepted")
	lock, err := acquireLock(lockPath, accepted)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	raw, err := os.ReadFile(accepted)
	if err != nil || string(raw) != "accepted\n" {
		t.Fatalf("launch ack=%q, %v", raw, err)
	}
	busy := filepath.Join(dir, "busy")
	other, err := acquireLock(lockPath, busy)
	if err == nil {
		other.Close()
		t.Fatal("second lock acquisition succeeded")
	}
	raw, err = os.ReadFile(busy)
	if err != nil || string(raw) != "busy\n" {
		t.Fatalf("busy ack=%q, %v", raw, err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	// An expired launch acknowledgment must fail and release the newly taken
	// lock, so a worker cannot execute after its dispatcher abandoned launch.
	expired := filepath.Join(dir, "expired", "ack")
	other, err = acquireLock(lockPath, expired)
	if err == nil {
		other.Close()
		t.Fatal("missing ack directory accepted")
	}
	other, err = acquireLock(lockPath, "")
	if err != nil {
		t.Fatalf("failed acknowledgment retained lock: %v", err)
	}
	other.Close()
}

func TestLockSurvivesWorkerDeath(t *testing.T) {
	if dir := os.Getenv("KANEO_LOCK_HELPER"); dir != "" {
		lock, err := acquireLock(filepath.Join(dir, "worker.lock"), "")
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		_, err = process(t.Context(), lock, dir, "", os.Environ(), "sh", "-c", "echo $$ > child-pid; sleep 30")
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	helper := exec.Command(executable, "-test.run=^TestLockSurvivesWorkerDeath$")
	helper.Env = append(os.Environ(), "KANEO_LOCK_HELPER="+dir)
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = helper.Process.Kill(); _ = helper.Wait() }()
	child := 0
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(filepath.Join(dir, "child-pid"))
		if err == nil {
			child, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
			if child > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if child == 0 {
		t.Fatal("helper never started its supervised child")
	}
	defer unix.Kill(-child, unix.SIGKILL)
	if err := helper.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = helper.Wait()
	if lock, err := acquireLock(filepath.Join(dir, "worker.lock"), ""); err == nil {
		lock.Close()
		t.Fatal("worker death released lock while child survived")
	}
	if err := unix.Kill(-child, unix.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if lock, err := acquireLock(filepath.Join(dir, "worker.lock"), ""); err == nil {
			lock.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("lock remained held after surviving child exited")
}
