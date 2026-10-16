package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/google/uuid"

	"github.com/Rezaft2013/event-real-time-processor/internal/database"
	kafkaLib "github.com/Rezaft2013/event-real-time-processor/internal/kafka"
	"github.com/Rezaft2013/event-real-time-processor/internal/model"
)

// HttpCreateEventHandler handles POST /api/v1/events post request
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

	// this is used as idempotency key in the system
	evt.ID = uuid.New()
	if err := kafkaLib.WriteEventToTopic("events", &evt); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, err = fmt.Fprintf(w, "{ \"event_id\" : \"%s\" }", evt.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// HttpGetEventHandler handles GET /api/v1/events/{id} request
// this endpoint added to be used in integration tests
func HttpGetEventHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	eventID := req.PathValue("id")
	_, result := database.CheckEventExists(eventID)
	if result {
		w.WriteHeader(http.StatusOK)
		_, err := fmt.Fprintf(w, "{ \"status\" : \"ok\" }")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNotFound)
	_, err := fmt.Fprintf(w, "{ \"status\" : \"not found\" }")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
