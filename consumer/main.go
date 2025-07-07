package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// --- Kafka Consumer Configuration ---
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "my-analytics-app", // Consumer group ID
		"auto.offset.reset": "earliest",         // Start from the beginning of the topic
	})

	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer c.Close()

	topic := "user-activity"
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	fmt.Println("Starting consumer... Waiting for messages.")
	// --- Message Consumption Loop ---
	for {
		msg, err := c.ReadMessage(-1) // Block until a message is received
		if err == nil {
			fmt.Printf("Received message from %s: Key: %s | Value: %s\n", msg.TopicPartition, string(msg.Key), string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
