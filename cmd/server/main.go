package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Rezaft2013/event-real-time-processor/internal/handlers"
	"github.com/Rezaft2013/event-real-time-processor/internal/kafka"

	pb "github.com/Rezaft2013/event-real-time-processor/api/proto/v1"
	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/events/{id}", handlers.HttpGetEventHandler)
	mux.HandleFunc("/api/v1/events", handlers.HttpCreateEventHandler)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	pid := syscall.Getpid()
	go func() {
		log.Println("http server listening at port 88")
		err := http.ListenAndServe(":88", mux)
		if err != nil {
			log.Printf("Server pid %d Error=%s\n", pid, err.Error())
		}
	}()

	grpcServer := grpc.NewServer()
	eventHandler := handlers.NewEventGRPCHandler()
	pb.RegisterEventServiceServer(grpcServer, eventHandler)

	go func() {
		log.Printf("gRPC server listening at %v", listener.Addr())
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	go kafka.Consumer(ctx)

	log.Printf("Server pid %d started.", pid)
	<-done

	grpcServer.GracefulStop()
	log.Printf("Server pid %d shut down.", pid)
}
