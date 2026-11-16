package main

import (
	"cmp"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

type queuedCard struct {
	card      card
	column    string
	stage     stage
	repo      string
	enteredAt time.Time
	priority  int
	detail    string
}

func priorityRank(priority string) (int, error) {
	switch priority {
	case "urgent":
		return 0, nil
	case "high":
		return 1, nil
	case "medium":
		return 2, nil
	case "low":
		return 3, nil
	case "", "no-priority":
		return 4, nil
	default:
		return 0, fmt.Errorf("unknown priority %q", priority)
	}
}

func compareQueuedCards(a, b queuedCard) int {
	if n := cmp.Compare(a.priority, b.priority); n != 0 {
		return n
	}
	if n := a.enteredAt.Compare(b.enteredAt); n != 0 {
		return n
	}
	return strings.Compare(a.card.ID, b.card.ID)
}

// Unacknowledged commands reserve their stage across dispatcher restarts and overlapping replicas.
func (d *dispatcher) stageAvailable(cm *corev1.ConfigMap, id, stage string) error {
	for key := range cm.Data {
		if key == id || strings.HasPrefix(key, "move.") || strings.HasPrefix(key, "reverted.") {
			continue
		}
		command, err := readCommand(cm, key)
		if err != nil {
			return err
		}
		if command == nil || command.Stage != stage {
			continue
		}
		if command.MergePR > 0 && command.Outcome != "merge-succeeded" && command.Outcome != "merge-reverted" {
			return errPodBusy
		}
		if command.Phase == "starting" || command.Phase == "reporting" {
			return errPodBusy
		}
		if command.Phase == "issued" && time.Since(command.IssuedAt) < d.unclaimed {
			return errPodBusy
		}
	}
	return nil
}
