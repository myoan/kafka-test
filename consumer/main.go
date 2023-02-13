package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

func ConnectConsumer(urls []string) (conn sarama.Consumer, err error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	conn, err = sarama.NewConsumer(urls, config)
	if err != nil {
		return nil, err
	}
	return
}

func main() {
	urls := []string{"localhost:9092"}
	c, err := ConnectConsumer(urls)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	pc, err := c.ConsumePartition("my-topic", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal(err)
	}
	for msg := range pc.Messages() {
		fmt.Printf("msg(%s): %s[%s]\n", msg.Topic, msg.Value, msg.Timestamp)
	}
}
