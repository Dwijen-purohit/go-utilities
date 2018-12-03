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
	concurrency  *int
	messageCount *int
	topic        *string
	brokers      *string
	brokerIPs    []string
)

func init() {
	ack = flag.Int("ack", -1, "required acknowledgement default is -1")
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

	for i := 0; i < msgCount; i++ {
		//		tStart := time.Now()
		err := w.WriteMessages(context.Background(), kg.Message{
			Key:   []byte(id),
			Value: []byte(fmt.Sprintf("message-%s-%d", id, i)),
		})
		if err != nil {
			fmt.Println("Err - - - -", err)
		}

		//	d := time.Now().Sub(tStart)
		//	fmt.Printf("Time taken per write message | Writer: %s | Duration: %s \n", id, d.String())
		stats := w.Stats()
		fmt.Printf("DialTime: %v | WriteTime: %v | WaitTime: %v | Writer: %s \n", stats.DialTime.Avg.String(), stats.WriteTime.Avg.String(), stats.WaitTime.Avg.String(), id)
	}
}
