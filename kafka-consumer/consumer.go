package kafkaconsumer

import (
	"fmt"
	"os"
	"log"
	"strings"
	"runtime"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/urfave/cli"
)

func KafkaConsumer (c *cli.Context) error {

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	brokers := strings.Split(c.String(ArgBrokerList), ",")
	topic := c.String(ArgTopic)
	partition := c.Int(ArgPartition)

	runtime.GOMAXPROCS(opts.GetCPU())

	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Create new consumer error %v", err)
	}

	defer func() {
		log.Printf("Closing the channel")
		if err := master.Close(); err != nil {
			log.Fatalf("Close error %v", err)
		}
	}()

	consumer, err := master.ConsumePartition(topic, int32(partition), sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("ConsumePartition error %v", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Println("Received messages", string(msg.Key), string(msg.Value))
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
	return nil
}

