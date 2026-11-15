package main

import (
	"context"

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
	next, err := fsmEvent(ctx, newWorkflowFSM(phase), event)
	if err != nil {
		return phase, err
	}
	return workflowPhase(next), nil
}
