package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// UserActivity represents a user interaction event
type UserActivity struct {
	UserID    string    `json:"user_id"`
	Page      string    `json:"page"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// --- Kafka Producer Configuration ---
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	// Go-routine to handle delivery reports
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	topic := "user-activity"
	users := []string{"user-1", "user-2", "user-3", "user-4", "user-5"}
	pages := []string{"/home", "/products/1", "/products/2", "/cart", "/checkout"}

	fmt.Println("Starting producer...")
	// --- Message Production Loop ---
	for {
		// 1. Select a random user and page
		randUser := users[rand.Intn(len(users))]
		randPage := pages[rand.Intn(len(pages))]

		// 2. Create the event data
		activity := UserActivity{
			UserID:    randUser,
			Page:      randPage,
			Timestamp: time.Now(),
		}

		// 3. Marshal the data to JSON
		value, err := json.Marshal(activity)
		if err != nil {
			log.Printf("Failed to marshal activity: %v", err)
			continue
		}

		// 4. Produce the message to the Kafka topic
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
			Key:            []byte(activity.UserID), // Keying by UserID ensures events from the same user go to the same partition
		}, nil)

		// Wait for all messages to be sent
		p.Flush(15 * 1000)
		time.Sleep(2 * time.Second)
	}
}
