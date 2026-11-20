package events

import (
	"context"
	"encoding/json"

	"example.com/scanner_demo/scanner/internal/database/model"
	"github.com/redis/go-redis/v9"
)

type EventPublisher struct {
	client *redis.Client
}

func NewEventPublisher(client *redis.Client) *EventPublisher {
	return &EventPublisher{
		client: client,
	}
}

func (p *EventPublisher) Publish(
	ctx context.Context,
	event *model.ScannerEvent,
) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: "scanner:events",
		ID:     "*",
		Values: map[string]any{
			"data": string(payload),
		},
	}).Err()
}
