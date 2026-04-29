package kafka

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kslog"
)

type Producer interface {
	Publish(ctx context.Context, deviceID string, message []byte) error
}

type KafkaProducer struct {
	client *kgo.Client
	topic  string
}

func (p *KafkaProducer) Publish(ctx context.Context, deviceID string, message []byte) error {
	record := &kgo.Record{Topic: p.topic, Value: message, Key: []byte(deviceID)}
	errorCh := make(chan error, 1)
	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		errorCh <- errors.New("error producing message")
	})
	return <-errorCh
}

func NewConsumer(ctx context.Context, topic string) (*kgo.Client, error) {
	sl := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.WithContext(ctx),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("event-logging"),
		kgo.WithLogger(kslog.New(sl)),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return cl, nil
}

func NewProducer(ctx context.Context, topic string) (*KafkaProducer, error) {
	sl := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.WithContext(ctx),
		kgo.WithLogger(kslog.New(sl)),
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		return nil, err
	}
	adminClient := kadm.NewClient(cl)
	adminClient.CreateTopic(ctx, 10, 2, nil, topic)
	return &KafkaProducer{client: cl, topic: topic}, nil
}
