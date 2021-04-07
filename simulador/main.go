package main

import (
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"log"
	appkafka "simulator/app/kafka"
	infkafka "simulator/infra/kafka"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("error loading .env file")
	}
}

func main() {

	msgChan := make(chan *ckafka.Message)
	consumer := infkafka.NewKafkaConsumer(msgChan)

	go consumer.Consume()

	for msg := range msgChan {
		fmt.Println(string(msg.Value))
		go appkafka.Produce(msg)
	}

}
