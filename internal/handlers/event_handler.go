package handlers

import (
	"context"
	"log"
	"net/http"
	"encoding/json"

	kafka "github.com/segmentio/kafka-go"
	"github.com/Rezaft2013/event-real-time-processor/internal/model"
)

func EventHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}	
	
	var evt model.Event
	err := json.NewDecoder(req.Body).Decode(&evt)
		if err != nil {
			log.Println("error=", err)
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}		
	log.Printf("event received %v\n", evt)

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"broker:29092"},
		Topic:   "events-topic",
	})
	defer writer.Close()
	
	data, err:= json.Marshal(evt)
	if err!=nil{
		log.Fatal(err)
	}		

	message := kafka.Message{
		Key:   []byte(evt.TenantID),
		Value: data,
	}
	if err := writer.WriteMessages(context.Background(), message); err != nil {
		log.Fatal("Failed to write message:", err)
	}
	log.Println("event successfully was written to kafka topic")
}