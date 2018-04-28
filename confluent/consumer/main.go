// Example function-based high-level Apache Kafka consumer
package main

/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// consumer_example implements a consumer using the non-channel Poll() API
// to retrieve messages and events.

import (
    "fmt"
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "os"
    "os/signal"
    "syscall"
    "sync/atomic"
    "time"
)

//finished in  1667 ms
func main() {

    if len(os.Args) < 4 {
        fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
            os.Args[0])
        os.Exit(1)
    }

    broker := os.Args[1]
    group := os.Args[2]
    topics := os.Args[3:]
    sigchan := make(chan os.Signal, 1)
    signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

    c, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers":    broker,
        "group.id":             group,
        "session.timeout.ms":   6000,
        "auto.offset.reset":    "beginning",
        "default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"}})

    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
        os.Exit(1)
    }

    fmt.Printf("Created Consumer %v\n", c)

    err = c.SubscribeTopics(topics, nil)

    run := true

    var counter uint64 = 0

    //for {
    //    msg, err := c.ReadMessage(-1)
    //    if err == nil {
    //        fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
    //        atomic.AddUint64(&counter, 1)
    //        fmt.Println("consumed ", counter)
    //    } else {
    //        fmt.Printf("Consumer error: %v (%v)\n", err, msg)
    //        break
    //    }
    //}

    var startTime time.Time

    for run == true {
        select {
        case sig := <-sigchan:
            fmt.Printf("Caught signal %v: terminating\n", sig)
            run = false
        default:
            ev := c.Poll(100)
            if ev == nil {
                continue
            }

            switch e := ev.(type) {
            case *kafka.Message:
                if counter == 0 {
                    startTime = time.Now()
                }
                atomic.AddUint64(&counter, 1)
                //fmt.Println("consumed ", counter)
                //fmt.Printf("%% Message on %s:\n%s\n",
                //    e.TopicPartition, string(e.Value))
                if e.Headers != nil {
                    fmt.Printf("%% Headers: %v\n", e.Headers)
                }
                if counter % 10000 == 0 {
                    fmt.Println(counter)
                }
                if counter >= 1000000 {
                    fmt.Println("finished in ", time.Now().Sub(startTime).Nanoseconds() / 1000000, "ms")
                    break
                }
            case kafka.PartitionEOF:
                //fmt.Printf("%% Reached %v\n", e)
            case kafka.Error:
                fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
                run = false
            default:
                fmt.Printf("Ignored %v\n", e)
            }
        }
    }

    fmt.Printf("Closing consumer\n")
    c.Close()
    time.Sleep(1 * time.Second)
}