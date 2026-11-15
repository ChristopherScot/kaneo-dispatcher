package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/looplab/fsm"
)

type workflowPhase string

const (
	readyChild       workflowPhase = "ready-child"
	awaitingChild    workflowPhase = "awaiting-child"
	childrenComplete workflowPhase = "children-complete"

	activateChildEvent  = "child-activated"
	advanceChildEvent   = "child-merged"
	finishChildrenEvent = "last-child-merged"
)

// newWorkflowFSM is the single transition table used by execution and the
// Graphviz generator. Each decision gets its own in-memory FSM, initialized
// from durable state. Callbacks never perform persistence or external writes.
func newWorkflowFSM(initial workflowPhase) *fsm.FSM {
	return fsm.NewFSM(string(initial), fsm.Events{
		{Name: activateChildEvent, Src: []string{string(readyChild)}, Dst: string(awaitingChild)},
		{Name: advanceChildEvent, Src: []string{string(readyChild), string(awaitingChild)}, Dst: string(readyChild)},
		{Name: finishChildrenEvent, Src: []string{string(readyChild), string(awaitingChild)}, Dst: string(childrenComplete)},
	}, nil)
}

func (phase workflowPhase) transition(ctx context.Context, event string) (workflowPhase, error) {
	machine := newWorkflowFSM(phase)
	err := machine.Event(ctx, event)
	// An already-completed child can advance the cursor before activation was
	// observed. The library validates that ready -> ready event but reports it
	// as NoTransitionError because the phase itself stays unchanged.
	var unchanged fsm.NoTransitionError
	if errors.As(err, &unchanged) && unchanged.Err == nil {
		err = nil
	}
	if err != nil {
		return phase, fmt.Errorf("workflow event %s: %w", event, err)
	}
	return workflowPhase(machine.Current()), nil
}
