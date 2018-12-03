package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	kg "github.com/segmentio/kafka-go"
)

var (
	ack          *int
	batchSize    *int
	concurrency  *int
	messageCount *int
	topic        *string
	brokers      *string
	brokerIPs    []string
)

func init() {
	ack = flag.Int("ack", -1, "required acknowledgement default is -1")
	batchSize = flag.Int("batch-size", 1, "default batchSize is 1")
	concurrency = flag.Int("concurrency", 4, "number of concurrent requests")
	messageCount = flag.Int("message-count", 100, "number of messages")
	topic = flag.String("topic", "", "topic to test")
	brokers = flag.String("brokers", "", "comma separated kafka brokers to connect to")

	flag.Parse()

	brokerIPs = strings.Split(*brokers, ",")
}

func main() {
	fmt.Printf("Opts = concurrency: %d | message-count: %d | topic: %s | brokers: %s \n", *concurrency, *messageCount, *topic, *brokers)

	write(fmt.Sprintf("writer-%d", 1), 10)
}

func write(id string, msgCount int) {
	w := kg.NewWriter(kg.WriterConfig{
		Brokers:      brokerIPs,
		Topic:        *topic,
		RequiredAcks: *ack,
	})
	defer w.Close()

	for i := 0; i < msgCount; i++ {
		st := time.Now()

		err := w.WriteMessages(context.Background(), kg.Message{
			Key:   []byte(id),
			Value: []byte(fmt.Sprintf("message-%s-%d", id, i)),
		})
		et := time.Since(st).Seconds()
		if err != nil {
			fmt.Println("Err - - - -", err)
		}
	}
}
