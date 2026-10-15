package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Rezaft2013/event-real-time-processor/internal/database"
	"github.com/Rezaft2013/event-real-time-processor/internal/model"
	"github.com/Rezaft2013/event-real-time-processor/internal/schema_validation"
	"github.com/segmentio/kafka-go"
)

func Consumer(ctx context.Context) {
	time.Sleep(5 * time.Second)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"broker:29092"},
		Topic:   "events",
		GroupID: "group-id-1",
		MaxWait: 500 * time.Millisecond,
	})
	defer reader.Close()

	fmt.Println("Kafka consumer listening...")
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("kafka consumer error in fetching message, error: %v. exiting ...", err)
			return
		}

		var event model.Event
		json.Unmarshal(msg.Value, &event)
		fmt.Printf("Kafka consumer fetched event: %v\n", event)

		retryCount := 0
		maxRetry := 5
		for {
			retryCount = retryCount + 1

			err = processEvent(&event)
			if err == nil {
				fmt.Printf("Kafka consumer processed event: %v\n", event)
				break
			} else {
				fmt.Printf("Kafka consumer error: event:%v, error=%s\n", event, err)
			}

			if retryCount >= maxRetry {
				fmt.Printf("Kafka consumer: too many errors processing event: %v, writing it to dead-letter topic\n", event)
				insertToDeadLetterQueue(&event)
				break
			}
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("failed to commit kafka message for event:%v in events-topic. error: %s", event, err)
		}
	}
}

func processEvent(evt *model.Event) error {
	validator := schema_validation.NewSchemaValidator()
	validationErr := validator.Validate(evt.Version, evt.Payload)
	err := database.InsertEventToDB(evt, validationErr)
	if err != nil {
		return fmt.Errorf("error writing event to db. error: %s\n", err)
	}

	if validationErr != nil {
		return nil
	}

	err = insertEventIntoSenderTopic(evt)
	if err != nil {
		return fmt.Errorf("error writing event to sender topic. error: %s\n", err)
	}
	return nil
}

func insertToDeadLetterQueue(evt *model.Event) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"broker:29092"},
		Topic:   "events-deadletter",
	})
	writer.AllowAutoTopicCreation = true
	defer writer.Close()

	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	message := kafka.Message{
		Key:   []byte(evt.TenantID),
		Value: data,
	}
	if err := writer.WriteMessages(context.Background(), message); err != nil {
		log.Println("Failed to write message to events-deadleeter-topic:", err)
		return err
	}
	log.Println("event successfully was written to kafka events-deadleeter-topic topic")
	return nil
}

func insertEventIntoSenderTopic(evt *model.Event) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"broker:29092"},
		Topic:   "sender",
	})
	writer.AllowAutoTopicCreation = true
	defer writer.Close()

	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	message := kafka.Message{
		Key:   []byte(evt.TenantID),
		Value: data,
	}
	if err := writer.WriteMessages(context.Background(), message); err != nil {
		log.Println("Failed to write message to sender topic:", err)
		return err
	}
	log.Printf("event successfully was written to kafka sender topic")
	return nil
}
