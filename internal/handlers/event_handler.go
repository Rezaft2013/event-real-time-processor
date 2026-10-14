package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/Rezaft2013/event-real-time-processor/internal/model"
	kafka "github.com/segmentio/kafka-go"
)

func EventHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var evt model.Event
	err := json.NewDecoder(req.Body).Decode(&evt)
	if err != nil {
		log.Println("error=", err)
		http.Error(w, "invalid json payload", http.StatusBadRequest)
		return
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"broker:29092"},
		Topic:   "events-topic",
	})
	writer.AllowAutoTopicCreation = true
	defer writer.Close()

	data, err := json.Marshal(evt)
	if err != nil {
		log.Fatal(err)
	}

	message := kafka.Message{
		Key:   []byte(evt.TenantID),
		Value: data,
	}
	if err := writer.WriteMessages(context.Background(), message); err != nil {
		log.Fatal("failed to write message to events-topic topic:", err)
	}

	log.Println("event successfully was written to kafka events-topic topic")
}
