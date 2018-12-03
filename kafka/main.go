package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"

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

	msgPerWriter := *messageCount / *concurrency
	wg := sync.WaitGroup{}

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go write(fmt.Sprintf("writer-%d", i), msgPerWriter, &wg)
	}

	wg.Wait()
}

func write(id string, msgCount int, wg *sync.WaitGroup) {
	defer wg.Done()

	w := kg.NewWriter(kg.WriterConfig{
		Brokers:      brokerIPs,
		Topic:        *topic,
		RequiredAcks: *ack,
	})
	defer w.Close()

	group := []kg.Message{}

	for i := 0; i < msgCount; i++ {
		if len(group) < *batchSize {
			group = append(group, kg.Message{
				Key:   []byte(id),
				Value: []byte(fmt.Sprintf("message-%s-%d", id, i)),
			})

			continue
		}

		err := w.WriteMessages(context.Background(), group...)
		if err != nil {
			fmt.Println("Err - - - -", err)
		}

		stats := w.Stats()
		fmt.Printf("DialTime: %v | WriteTime: %v | WaitTime: %v | Writer: %s \n", stats.DialTime.Avg.String(), stats.WriteTime.Avg.String(), stats.WaitTime.Avg.String(), id)

		group = []kg.Message{}
	}
}
