package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/looplab/fsm"
)

var ticketColumns = []string{"backlog", "to-do", "in-progress", "in-review", "mergeable", "needs-human-review", "done"}

// Workers report facts. Only this table chooses their resulting board column.
// Manual drags are observations, not permission for a worker to move a card.
func newTicketFSM(initial string) *fsm.FSM {
	return fsm.NewFSM(initial, fsm.Events{
		{Name: "child-queued", Src: []string{"backlog"}, Dst: "to-do"},
		{Name: "parent-started", Src: []string{"to-do"}, Dst: "in-progress"},
		{Name: "parent-completed", Src: []string{"in-progress"}, Dst: "done"},
		{Name: "author-started", Src: []string{"to-do", "in-progress"}, Dst: "in-progress"},
		{Name: "author-succeeded", Src: []string{"in-progress"}, Dst: "in-review"},
		{Name: "author-failed", Src: []string{"in-progress"}, Dst: "backlog"},
		{Name: "author-blocked", Src: []string{"in-progress"}, Dst: "needs-human-review"},
		{Name: "test-started", Src: []string{"in-review"}, Dst: "in-review"},
		{Name: "test-succeeded", Src: []string{"in-review"}, Dst: "mergeable"},
		{Name: "test-failed", Src: []string{"in-review"}, Dst: "to-do"},
		{Name: "test-blocked", Src: []string{"in-review"}, Dst: "needs-human-review"},
		{Name: "merge-started", Src: []string{"mergeable"}, Dst: "mergeable"},
		{Name: "merge-succeeded", Src: []string{"mergeable"}, Dst: "done"},
		{Name: "merge-failed", Src: []string{"mergeable"}, Dst: "to-do"},
		{Name: "merge-blocked", Src: []string{"mergeable"}, Dst: "needs-human-review"},
		{Name: "merge-reverted", Src: []string{"mergeable"}, Dst: "to-do"},
		{Name: "merge-observed", Src: []string{"to-do", "in-progress", "in-review", "mergeable", "needs-human-review"}, Dst: "done"},
	}, nil)
}

func fsmEvent(ctx context.Context, machine *fsm.FSM, event string) (string, error) {
	err := machine.Event(ctx, event)
	var unchanged fsm.NoTransitionError
	if errors.As(err, &unchanged) && unchanged.Err == nil {
		err = nil
	}
	if err != nil {
		return "", fmt.Errorf("event %s: %w", event, err)
	}
	return machine.Current(), nil
}

// Kaneo has already committed a manual drag before delivering its webhook.
// Every known column is an allowed operator destination, including pauses and
// reopening Done. Automation subsequently applies its own guards.
func newObservationFSM(initial string) *fsm.FSM {
	events := make(fsm.Events, 0, len(ticketColumns))
	for _, column := range ticketColumns {
		events = append(events, fsm.EventDesc{Name: "observe-" + column, Src: ticketColumns, Dst: column})
	}
	return fsm.NewFSM(initial, events, nil)
}

func newCommandFSM(initial string) *fsm.FSM {
	return fsm.NewFSM(initial, fsm.Events{
		{Name: "start", Src: []string{"issued"}, Dst: "starting"},
		{Name: "started", Src: []string{"starting"}, Dst: "running"},
		{Name: "report", Src: []string{"running"}, Dst: "reporting"},
		{Name: "reported", Src: []string{"reporting"}, Dst: "finished"},
	}, nil)
}
