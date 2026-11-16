package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ChristopherScot/kaneo-dispatcher/internal/worker"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: kaneo-worker <author|test|merge> <task> <owner/repo> <command>")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := worker.Run(ctx, worker.Environment(os.Args[1], os.Args[2], os.Args[3], os.Args[4])); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
