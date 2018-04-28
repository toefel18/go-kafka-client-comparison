// Example function-based Apache Kafka producer
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

import (
    "fmt"
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "os"
    "strconv"
    "time"
)

func main() {

    if len(os.Args) != 3 {
        fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic>\n",
            os.Args[0])
        os.Exit(1)
    }

    broker := os.Args[1]
    topic := os.Args[2]

    p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

    if err != nil {
        fmt.Printf("Failed to create producer: %s\n", err)
        os.Exit(1)
    }

    fmt.Printf("Created Producer %v\n", p)

    // Optional delivery channel, if not specified the Producer object's
    // .Events channel is used.
    deliveryChan := make(chan kafka.Event)

    value := "Hello Go!"

    for i := 0; i < 1000000; i++ {
        msg  := value + strconv.Itoa(i)
        err = p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Value:          []byte(msg),
            Headers:        []kafka.Header{{"myTestHeader", []byte("header values are binary")}},
        }, nil)

        if err != nil {
            fmt.Println(err.Error())
        }

        //e := <-deliveryChan
        //m := e.(*kafka.Message)
        //if m.TopicPartition.Error != nil {
        //    fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
        //} else {
        //    fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
        //        *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
        //}
        if i % 1000 == 0 {
            fmt.Println("produced ", i)
            time.Sleep(20 * time.Millisecond)
        }

    }
    fmt.Println("Finished")

    time.Sleep(5 * time.Second)

    //e := <-deliveryChan
    //m := e.(*kafka.Message)

    //if m.TopicPartition.Error != nil {
    //    fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
    //} else {
    //    fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
    //        *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
    //}

    close(deliveryChan)
}