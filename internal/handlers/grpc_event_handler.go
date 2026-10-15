package handlers

import (
	"context"
	pb "github.com/Rezaft2013/event-real-time-processor/api/proto/v1"
	kafkaLib "github.com/Rezaft2013/event-real-time-processor/internal/kafka"
	"github.com/Rezaft2013/event-real-time-processor/internal/model"
	"github.com/google/uuid"
)

type EventGRPCHandler struct {
	pb.UnimplementedEventServiceServer
}

func NewEventGRPCHandler() *EventGRPCHandler {
	return &EventGRPCHandler{}
}

// CreateEvent handles create event grpc call
func (h *EventGRPCHandler) CreateEvent(ctx context.Context, req *pb.CreateEventRequest) (*pb.CreateEventResponse, error) {
	var evt model.Event
	reqUid, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, err
	}
	evt.ID = reqUid
	evt.Type = req.Type
	evt.TenantID = req.TenantId
	evt.Timestamp = req.EventTimestamp.GetSeconds()
	evt.Version = req.Version
	evt.Payload = req.Payload

	if err := evt.Validate(); err != nil {
		return nil, err
	}

	if kafkaLib.WriteEventToTopic("events", &evt) != nil {
		return nil, err
	}
	return &pb.CreateEventResponse{}, nil
}
