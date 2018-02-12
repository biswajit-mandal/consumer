package main

import (
	"log"
	"os"

	kc "github.com/Juniper/ipfix-consumer/kafka-consumer"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "Kafka Consumer CLI"
	app.Commands = []cli.Command{
		{
			Name:  "kafka",
			Usage: "Kafka Consumer",
			Flags: []cli.Flag{
				cli.StringFlag{Name: kc.ArgTopic, Value: "vflow.ipfix",
					Usage: "Grouping of messages what will be consumed"},
				cli.StringFlag{Name: kc.ArgBrokerList, Value: "127.0.0.1:9092",
					Usage: "The list of brokers (comma seperated)"},
				cli.IntFlag{Name: kc.ArgPartition, Value: 0,
					Usage: "Kafka topic partition"},
			},
			Action: kc.KafkaConsumer,
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Application error: %s", err)
	}
}
