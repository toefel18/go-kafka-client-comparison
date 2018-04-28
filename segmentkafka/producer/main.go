package main

import (
    "github.com/segmentio/kafka-go"
    "context"
    "fmt"
    "time"
)

func main() {
    topic := "many-partitions"
    // make a writer that produces to topic-A, using the least-bytes distribution
    w := kafka.NewWriter(kafka.WriterConfig{
        Brokers:       []string{"localhost:31001"},
        Topic:         topic,
        Balancer:      &kafka.LeastBytes{},
        BatchTimeout:  1000 * time.Nanosecond,
        BatchSize:     1000,
        QueueCapacity: 1000,
        RequiredAcks:  1,
        WriteTimeout:  1000 * time.Millisecond,
        Async:         false,
    })

    for i := 0; i < 1000000; i++ {
        //ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(2 * time.Millisecond))
        err := w.WriteMessages(context.Background(),
            kafka.Message{
                Value: []byte("Hello GO-segment!"),
            })
        if err != nil {
            fmt.Println(err.Error())
            break
        }
        //ctx.Err()
        fmt.Println("wrote ", i)
    }

    fmt.Println("done")

    time.Sleep(5 * time.Second)

    w.Close()
}
