package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Each tool gets its own process group. Cancellation and normal exit both
// clean up descendants before releasing the worker's pod lock.
func process(ctx context.Context, lock *os.File, dir, input string, env []string, name string, args ...string) (string, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(runCtx, name, args...)
	cmd.Dir, cmd.Env, cmd.Stdin = dir, env, bytes.NewBufferString(input)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Keep the lock alive if this worker is killed before it can reap the
	// tool. A replacement must not overlap a surviving Claude process.
	if lock != nil {
		cmd.ExtraFiles = []*os.File{lock}
	}
	cmd.WaitDelay = 2 * time.Second
	stdout, stderr := limitedBuffer{cancel: cancel}, limitedBuffer{cancel: cancel}
	outReader, outWriter, err := os.Pipe()
	if err != nil {
		return "", err
	}
	defer outReader.Close()
	defer outWriter.Close()
	errReader, errWriter, err := os.Pipe()
	if err != nil {
		return "", err
	}
	defer errReader.Close()
	defer errWriter.Close()
	// Own these pipes so Wait observes the leader's exit immediately instead
	// of waiting for descendants to close inherited stdout/stderr handles.
	cmd.Stdout, cmd.Stderr = outWriter, errWriter
	cmd.Cancel = func() error {
		err := unix.Kill(-cmd.Process.Pid, unix.SIGKILL)
		if errors.Is(err, unix.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	if err := cmd.Start(); err != nil {
		return "", err
	}
	copied := make(chan error, 2)
	go func() { _, err := io.Copy(&stdout, outReader); copied <- err }()
	go func() { _, err := io.Copy(&stderr, errReader); copied <- err }()
	waitErr := cmd.Wait()
	cleanupErr := unix.Kill(-cmd.Process.Pid, unix.SIGKILL)
	if errors.Is(cleanupErr, unix.ESRCH) {
		cleanupErr = nil
	}
	closeErr := errors.Join(outWriter.Close(), errWriter.Close())
	// A tool that deliberately escaped its process group must not keep pipe
	// readers alive indefinitely. Ordinary descendants die with the group.
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	var copyErr error
	for range 2 {
		select {
		case err := <-copied:
			copyErr = errors.Join(copyErr, err)
		case <-timer.C:
			_ = outReader.Close()
			_ = errReader.Close()
			copyErr = errors.Join(copyErr, exec.ErrWaitDelay, <-copied)
		}
	}
	if stdout.exceeded || stderr.exceeded {
		return stdout.String(), errors.Join(errOutputLimit, cleanupErr)
	}
	if ctx.Err() != nil {
		return stdout.String(), ctx.Err()
	}
	if err := errors.Join(cleanupErr, closeErr, copyErr); err != nil {
		return stdout.String(), fmt.Errorf("clean up %s: %w", name, err)
	}
	if waitErr != nil {
		return stdout.String(), fmt.Errorf("%s: %w: %s", name, waitErr, tail(stderr.String(), 4000))
	}
	return stdout.String(), nil
}

var errOutputLimit = errors.New("tool output exceeds 8 MiB")

type limitedBuffer struct {
	buffer   bytes.Buffer
	cancel   context.CancelFunc
	exceeded bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.buffer.Len()+len(p) > 8<<20 {
		b.exceeded = true
		b.cancel()
		return 0, errOutputLimit
	}
	return b.buffer.Write(p)
}
func (b *limitedBuffer) String() string { return b.buffer.String() }

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

func writeNew(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// Persist each directory in its parent before relying on files beneath it.
// Existing paths repeat the barrier in case a prior creation was interrupted.
func makeDurableDirs(path string) error {
	err := os.Mkdir(path, 0700)
	if errors.Is(err, os.ErrNotExist) {
		if err := makeDurableDirs(filepath.Dir(path)); err != nil {
			return err
		}
		err = os.Mkdir(path, 0700)
	}
	if errors.Is(err, os.ErrExist) {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("%s is not a directory", path)
		}
		return syncDir(filepath.Dir(path))
	}
	if err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func acquireLock(path, ack string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			_ = acknowledge(ack, "busy")
		}
		return nil, fmt.Errorf("pod lock: %w", err)
	}
	if err = acknowledge(ack, "accepted"); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}
func acknowledge(path, value string) error {
	if path == "" {
		return nil
	}
	if err := os.WriteFile(path+".tmp", []byte(value+"\n"), 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// Avoid propagating worker RPC credentials or API billing overrides to tools.
func toolEnv(token, gh string) []string {
	env := make([]string, 0, len(os.Environ())+2)
	for _, e := range os.Environ() {
		key, _, _ := strings.Cut(e, "=")
		switch key {
		case "KANEO_API_KEY", "DISPATCH_API_KEY", "KANEO_API_URL", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL", "CLAUDE_CODE_OAUTH_TOKEN", "GH_TOKEN":
			continue
		}
		env = append(env, e)
	}
	env = append(env, "GH_TOKEN="+gh)
	if token != "" {
		env = append(env, "CLAUDE_CODE_OAUTH_TOKEN="+token)
	}
	return env
}

var _ io.Writer = (*limitedBuffer)(nil)
