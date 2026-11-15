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
	machine := newWorkflowFSM(readyChild)
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "ticket":
			machine = newTicketFSM("backlog")
		case "command":
			machine = newCommandFSM("issued")
		case "operator":
			machine = newObservationFSM("backlog")
		default:
			fmt.Fprintln(os.Stderr, "unknown graph")
			os.Exit(1)
		}
	}
	if _, err := os.Stdout.WriteString(fsm.Visualize(machine)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
