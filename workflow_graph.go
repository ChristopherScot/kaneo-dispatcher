//go:build ignore

// Run with workflow_fsm.go via make graph. This uses the production FSM table,
// not a second copy of its states and edges.
package main

import (
	"fmt"
	"os"

	"github.com/looplab/fsm"
)

func main() {
	if _, err := os.Stdout.WriteString(fsm.Visualize(newWorkflowFSM(readyChild))); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
