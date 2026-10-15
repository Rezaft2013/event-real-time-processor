package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/Rezaft2013/event-real-time-processor/internal/database"
	kafkaLib "github.com/Rezaft2013/event-real-time-processor/internal/kafka"
	"github.com/Rezaft2013/event-real-time-processor/internal/model"
)

func HttpCreateEventHandler(w http.ResponseWriter, req *http.Request) {
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

	if evt.Validate() != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := kafkaLib.WriteEventToTopic("events", &evt); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "event received.")
}

func HttpGetEventHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	eventID := req.PathValue("id")
	_, result := database.CheckEventExists(eventID)
	if result {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "{ \"status\" : \"ok\" }")
		return
	}

	w.WriteHeader(http.StatusNotFound)
	fmt.Fprintf(w, "{ \"status\" : \"not found\" }")
}
