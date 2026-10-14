package kafka 

import(
	"context"
	"log"
	"fmt"
	"time"
	"encoding/json"

	"github.com/segmentio/kafka-go"
	"github.com/Rezaft2013/event-real-time-processor/internal/model"	
	"github.com/Rezaft2013/event-real-time-processor/internal/database"
)

func Consumer(){
	time.Sleep(5 * time.Second)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"broker:29092"},
		Topic:   "events-topic",		
	})
	defer reader.Close()

	fmt.Println("Kafka Consumer listening...")
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("Kafka consumer error:", err)
		}
		
		var event model.Event
		json.Unmarshal(msg.Value, &event)	
		fmt.Printf("Kafka consumer fetched event: %v\n", event)	

		err = database.InsertToDB(&event) 
		if err != nil {
			fmt.Printf("Kafka consumer: error writing event to db: %v\n", event)	
		}
	}
}

