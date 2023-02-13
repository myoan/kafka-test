package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

func ConnectProducer(urls []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	conn, err := sarama.NewSyncProducer(urls, config)

	if err != nil {
		return nil, err
	}

	return conn, nil
}

func main() {
	urls := []string{"localhost:9092"}
	p, err := ConnectProducer(urls)
	if err != nil {
		log.Fatal(err)
	}

	defer p.Close()

	msg := &sarama.ProducerMessage{
		Topic: "my-topic",
		Value: sarama.StringEncoder([]byte("hogehgoe")),
	}

	part, offset, err := p.SendMessage(msg)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("message is stored in topics(%s)/partition(%d)/offset(%d)\n", "my-topic", part, offset)
}
