package main

import (
	"flag"
	"fmt"
	"strings"

	// kg "github.com/segmentio/kafka-go"
	"github.com/confluentinc/confluent-kafka-go/kafka"
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
	write2()
}

func write2() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": *brokers})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return
	}

	fmt.Printf("Created Producer %v\n", p)

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	meta, err := p.GetMetadata(topic)
	if err != nil {
		fmt.Printf("META ERR - %+v", err)
	}

	fmt.Printf("METADATA - %+v", meta)

	close(deliveryChan)
}

// func write(id string, msgCount int, wg *sync.WaitGroup) {
// 	defer wg.Done()
//
// 	w := kg.NewWriter(kg.WriterConfig{
// 		Brokers:      brokerIPs,
// 		Topic:        *topic,
// 		RequiredAcks: *ack,
// 	})
// 	defer w.Close()
//
// 	group := []kg.Message{}
//
// 	for i := 0; i < msgCount; i++ {
// 		if len(group) < *batchSize {
// 			group = append(group, kg.Message{
// 				Key:   []byte(id),
// 				Value: []byte(fmt.Sprintf("message-%s-%d", id, i)),
// 			})
//
// 			continue
// 		}
//
// 		err := w.WriteMessages(context.Background(), group...)
// 		if err != nil {
// 			fmt.Println("Err - - - -", err)
// 		}
//
// 		stats := w.Stats()
// 		fmt.Printf("DialTime: %v | WriteTime: %v | WaitTime: %v | Writer: %s \n", stats.DialTime.Avg.String(), stats.WriteTime.Avg.String(), stats.WaitTime.Avg.String(), id)
//
// 		group = []kg.Message{}
// 	}
// }
