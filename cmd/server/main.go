package main

import (	
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	
	"github.com/Rezaft2013/event-real-time-processor/internal/kafka"
	"github.com/Rezaft2013/event-real-time-processor/internal/handlers"
)

func main() {	
	go kafka.Consumer()
	http.HandleFunc("/api/v1/event", handlers.EventHandler)

	server := http.Server{
		Addr:        ":88",
		ReadTimeout: 10 * time.Second,
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	pid := syscall.Getpid()
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Printf("Server pid %d Error=%s\n", pid, err.Error())
		}
	}()

	log.Printf("Server pid %d started.", pid)
	<-done
	log.Printf("Server pid %d shut down.", pid)
}
