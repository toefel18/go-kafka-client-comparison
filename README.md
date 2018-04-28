# Kafka Golang library implementations

**GOAL**: compare golang kafka libs in usability, configurability and performance. 

The team I work in is building micro-services that communicate using Kafka. The services
that run on the JVM use the kafka-clients library, which is thoroughly tested and appears to work
when we had issues while running Kafka. 

Kafka did not come with an official library for Golang so we had to use an other available library.
In our case, we chose [shopify/sarama](https://github.com/Shopify/sarama) with [bsm/sarama-cluster](https://github.com/bsm/sarama-cluster) for consumer-group support. We have 
experienced some problems with these frameworks and the coding style differs a lot from the Java version.
Some problems:

 1. When adding extra partitions to a topic, the consumer did not rebalance and messages were lost.
    A restart could've fixed the problem, but the default `auto.offset.reset` is Newest (latest), so
    we never saw the previously produced messages on new topics.
    
 1. The library does not expose a poll-loop. Which makes it hard to take control over the message commit
    behaviour. We would like to commit after processing all the messages in every poll.
    
Recently, confluent published [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) which depends
on the C/C++ library [librdkafka](https://github.com/edenhill/librdkafka)

Please read [Getting Started](https://github.com/confluentinc/confluent-kafka-go#getting-started) 

# Results

The confluent library wins but Sarama is very close. Segmentio is very slow.
 
    
    
 