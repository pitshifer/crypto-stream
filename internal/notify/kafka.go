package notify

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

type Alert struct {
	Symbol     string    `json:"symbol"`
	Volatility float64   `json:"volatility"`
	Threshold  float64   `json:"threshold"`
	Timestamp  time.Time `json:"timestamp"`
}

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireOne,
		},
	}
}

func (p *Producer) Send(ctx context.Context, alert Alert) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return err
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(alert.Symbol),
		Value: payload,
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
