package kafka

import (
	"context"
	"encoding/json"
	"github.com/Rezaft2013/event-real-time-processor/internal/model"
	kafka "github.com/segmentio/kafka-go"
	"log"
)

// WriteEventToTopic writes an event to specified kafkak topic
func WriteEventToTopic(topic string, evt *model.Event) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"broker:29092"},
		Topic:   topic,
	})
	writer.AllowAutoTopicCreation = true
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("failed to close kafka %s topic writer, event=%v, error=%v", topic, evt, err)
		}
	}()

	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	message := kafka.Message{
		Key:   []byte(evt.TenantID),
		Value: data,
	}
	if err := writer.WriteMessages(context.Background(), message); err != nil {
		log.Printf("failed to write event %v to %s topic. error: %v", evt, topic, err)
		return err
	}

	log.Printf("event %v successfully was ingested to kafka %s topic", evt, topic)
	return nil
}
