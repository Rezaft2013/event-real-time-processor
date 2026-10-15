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

const (
	eventsTopic           = "events"
	eventsDeadletterTopic = "events-deadletter"
	senderTopic           = "sender"
)

// Consumer kafka events topic consumer to read events data to further processing
func Consumer(ctx context.Context) {
	time.Sleep(5 * time.Second)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"broker:29092"},
		Topic:   eventsTopic,
		GroupID: "group-id-1",
		MaxWait: 500 * time.Millisecond,
	})
	defer func() {
		err := reader.Close()
		if err != nil {
			log.Printf("error in closing events topic kafka reader. error = %v", err)
		}
	}()

	log.Println("Kafka consumer listening...")
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("kafka consumer error in fetching message, error: %v. exiting ...", err)
			return
		}

		var event model.Event
		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Printf("error in json unmarshalling event: %v, error=%v", event, err)
			continue
		}

		log.Printf("Kafka consumer fetched event: %v", event)

		retryCount := 0
		maxRetry := 5
		for {
			retryCount = retryCount + 1

			err = processEvent(&event)
			if err == nil {
				log.Printf("Kafka consumer processed event: %v", event)
				break
			} else {
				log.Printf("Kafka consumer error: event:%v, error=%s", event, err)
			}

			if retryCount >= maxRetry {
				log.Printf("Kafka consumer: too many errors processing event: %v, writing it to dead-letter topic\n", event)
				if err := WriteEventToTopic(eventsDeadletterTopic, &event); err != nil {
					log.Printf("failed to add to the events-deadletter queue, event:%v, error=%v", event, err)
				}
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
		return fmt.Errorf("error writing event to db. error: %s", err)
	}

	if validationErr != nil {
		return nil
	}

	err = WriteEventToTopic(senderTopic, evt)
	if err != nil {
		return fmt.Errorf("error writing event to sender topic. error: %v", err)
	}
	return nil
}