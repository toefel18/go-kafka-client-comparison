package main

import (
    "fmt"
    "log"
    "time"
    "github.com/Shopify/sarama"
    "strconv"
)

func main() {

    // For the data collector, we are looking for strong consistency semantics.
    // Because we don't change the flush settings, sarama will try to produce messages
    // as fast as possible to keep latency low.
    config := sarama.NewConfig()
    config.Producer.RequiredAcks = sarama.WaitForLocal // Wait for all in-sync replicas to ack the message
    config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
    config.Producer.Return.Successes = true

    // On the broker side, you may want to change the following settings to get
    // stronger consistency guarantees:
    // - For your broker, set `unclean.leader.election.enable` to false
    // - For the topic, you could increase `min.insync.replicas`.

    producer, err := sarama.NewAsyncProducer([]string{"localhost:31001"}, nil)
    if err != nil {
        log.Fatalln("Failed to start Sarama producer:", err)
    }

    startTime := time.Now()

    value := "Hello Go!"
    for i := 0; i < 1000000; i++ {
        msg := &sarama.ProducerMessage{
            Topic: "many-partitions",
            Key:   sarama.StringEncoder(strconv.Itoa(i)),
            Value: sarama.StringEncoder(value),
        }
        select {
        case producer.Input() <- msg:
        case err := <-producer.Errors():
            log.Println("Failed to produce message", err)
        }
        if i%10000 == 0 {
            fmt.Println("wrote ", i)
        }
    }

    fmt.Println("finished in ", time.Now().Sub(startTime).Nanoseconds()/1000000, "ms")

    producer.Close()
}
