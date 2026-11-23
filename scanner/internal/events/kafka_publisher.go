package events

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"example.com/scanner_demo/scanner/internal/database/model"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaPublisher struct {
	client *kgo.Client
	topic  string
	queue  chan model.ScannerEvent
}

func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowIdempotentProduceCancellation(),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{
		client: client,
		topic:  topic,
		queue:  make(chan model.ScannerEvent, 256),
	}, nil
}

func (p *KafkaPublisher) Publish(
	ctx context.Context,
	event *model.ScannerEvent,
) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(strconv.FormatInt(event.ChainID, 10)),
		Value: payload,
	}

	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *KafkaPublisher) Close() {
	p.client.Close()
}

func (p *KafkaPublisher) TryEnqueue(event model.ScannerEvent) bool {
	event.Data = bytes.Clone(event.Data)
	select {
	case p.queue <- event:
		return true
	default:
		return false
	}
}

func (p *KafkaPublisher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-p.queue:
			if ctx.Err() != nil {
				return
			}
			sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := p.Publish(
				sendCtx,
				&event,
			)
			cancel()
			if err != nil {
				slog.Warn("Kafka 发送失败",
					"event_id", event.EventID,
					"chain_id", event.ChainID,
					"error", err,
				)
			}
		}
	}
}
