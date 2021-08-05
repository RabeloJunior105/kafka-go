package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	delivyChan := make(chan kafka.Event)
	producer := NewKafkaProducer()

	publish("message", "teste", producer, nil, delivyChan)

	e := <-delivyChan
	response := e.(*kafka.Message)

	if response.TopicPartition.Error != nil {
		fmt.Println("Erro ao enviar a mensagem")
	} else {
		fmt.Println("Mensagem enviada com sucesso: ", response.TopicPartition)
	}
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-go_kafka_1:9092",
	}

	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func publish(
	msg string, topic string, producer *kafka.Producer, key []byte, delivyChan chan kafka.Event,
) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}

	err := producer.Produce(message, delivyChan)
	if err != nil {
		return err
	}

	return nil
}
