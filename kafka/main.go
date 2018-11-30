package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	kg "github.com/segmentio/kafka-go"
	"sync"
	"time"
)

var (
	concurrency  *int
	messageCount *int
	topic        *string
	brokers      *string
	brokerIPs    []string
)

func init() {
	concurrency = flag.Int("concurrency", 4, "number of concurrent requests")
	messageCount = flag.Int("message-count", 100, "number of messages")
	topic = flag.String("topic", "", "topic to test")
	brokers = flag.String("brokers", "", "comma separated kafka brokers to connect to")

	flag.Parse()

	brokerIPs = strings.Split(*brokers, ",")
}

func main() {
	fmt.Printf("Opts = concurrency: %d | message-count: %d | topic: %s | brokers: %s", *concurrency, *messageCount, *topic, *brokers)

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
		Brokers: brokerIPs,
		Topic:   *topic,
	})
	defer w.Close()

	for i := 0; i < msgCount; i++ {
		tStart := time.Now().Unix()
		w.WriteMessages(context.Background(), kg.Message{
			Key:   []byte(id),
			Value: []byte(fmt.Sprintf("message-%s-%d", id, i)),
		})
		fmt.Printf("Time taken per write message | Writer: %s | Duration: %d \n", id, time.Now().Unix() - tStart)
	}
}
