package main

import (
    "fmt"
    "log"
    "os"
    "os/signal"

    "github.com/bsm/sarama-cluster"
    "sync/atomic"
    "time"
    "github.com/Shopify/sarama"
)

//finished in  5654 ms
func main() {

    // init (custom) config, enable errors and notifications
    config := cluster.NewConfig()
    config.Consumer.Return.Errors = true
    config.Group.Return.Notifications = true
    config.Consumer.Offsets.Initial = sarama.OffsetOldest
    // init consumer
    brokers := []string{"localhost:31001"}
    topics := []string{"many-partitions"}
    consumer, err := cluster.NewConsumer(brokers, "sarama-perftest2", topics, config)
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    // trap SIGINT to trigger a shutdown.
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    // consume errors
    go func() {
        for err := range consumer.Errors() {
            log.Printf("Error: %s\n", err.Error())
        }
    }()

    // consume notifications
    go func() {
        for ntf := range consumer.Notifications() {
            log.Printf("Rebalanced: %+v\n", ntf)
        }
    }()

    var counter uint64
    var startTime time.Time
    // consume messages, watch signals
    for {
        select {
        case msg, ok := <-consumer.Messages():
            if ok {
                //fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
                consumer.MarkOffset(msg, "")	// mark message as processed
                consumer.CommitOffsets()
                if counter == 0 {
                    startTime = time.Now()
                }
                atomic.AddUint64(&counter, 1)
                if counter % 10000 == 0 {
                    fmt.Println(counter)
                }
                //fmt.Println("consumed ", counter)
                //fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
                if counter == 1000000 {
                    fmt.Println("finished in ", time.Now().Sub(startTime).Nanoseconds() / 1000000, "ms")

                    time.Sleep(1 * time.Second)
                    return
                }
            }
        case <-signals:
            return
        }
    }
    consumer.Close()
}