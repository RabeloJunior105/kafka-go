package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	delivyChan := make(chan kafka.Event)
	producer := NewKafkaProducer()

	Publish("Transferencia", "teste", producer, []byte("transfer"), delivyChan)

	go DeliveryReport(delivyChan)

	producer.Flush(1000)

}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "kafka-go_kafka_1:9092",
		"delivery.timeout.ms": "0",
	}

	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func Publish(
	msg string,
	topic string,
	producer *kafka.Producer,
	key []byte,
	delivyChan chan kafka.Event,
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

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar a mensagem", ev.TopicPartition.Error)
			} else {
				/*
					 anotar no banco de dados que a mensagem foi processada
						- Ex: confirmar que uma transferencia bancaria ocorreu.
				*/
				fmt.Println("Mensagem enviada com sucesso: ", ev.TopicPartition)
			}
		}
	}
}
