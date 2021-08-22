package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-go_kafka_1:9092",
		"client.id":         "go-app-consumer",
		"group.id":          "go-app-group",
	}
	consumer, err := kafka.NewConsumer(configMap)

	if err != nil {
		fmt.Println("Erro consumer", err.Error())
	}

	topics := []string{"teste"}

	consumer.SubscribeTopics(topics, nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {

			fmt.Println(string(msg.Value), msg.TopicPartition)
		} else {
			fmt.Println("Error", err.Error())
		}

	}
}
