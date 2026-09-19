package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func runCommand(ctx context.Context, args []string, store *EventStore) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}

	switch args[0] {
	case "replay-dead-letter":
		if len(args) != 2 || strings.TrimSpace(args[1]) == "" {
			return true, fmt.Errorf("uso: go run . replay-dead-letter <event-id>")
		}

		eventID := strings.TrimSpace(args[1])
		if err := store.ReplayDeadLetter(ctx, eventID); err != nil {
			return true, err
		}

		return true, json.NewEncoder(os.Stdout).Encode(map[string]any{
			"replayed": true,
			"eventId":  eventID,
		})
	default:
		return true, fmt.Errorf("comando desconhecido %q; uso: go run . replay-dead-letter <event-id>", args[0])
	}
}
