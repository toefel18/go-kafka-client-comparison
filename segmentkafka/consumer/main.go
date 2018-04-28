package main

import (
    "fmt"
    "github.com/segmentio/kafka-go"
    "context"
    "sync/atomic"
    "time"
)

// NOTE:   switch to branch fix/kafka_0_10_1 for older kafka version support
func main() {
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers:   []string{"localhost:31001"},
        GroupID:   "segmentio-perftest",
        Topic:     "many-partitions",
        //MinBytes:  10e3, // 10KB
        //MaxBytes:  10e6, // 10MB
    })
    //r.SetOffset(-2)
    var counter uint64
    var startTime time.Time
    for {
        _, err := r.ReadMessage(context.Background())
        if counter == 0 {
            startTime = time.Now()
        }
        if err != nil {
            fmt.Println(err.Error())
            break
        }
        atomic.AddUint64(&counter, 1)
        if counter % 10000 == 0 {
            fmt.Println(counter)
        }
        //fmt.Println("consumed ", counter)
        //fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
        if counter == 1000000 {
            fmt.Println("finished in ", time.Now().Sub(startTime).Nanoseconds() / 1000000, "ms")
            break
        }
    }

    r.Close()
    time.Sleep(1 * time.Second)
}
