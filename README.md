# Kafka-resources

Contains the basic template code for the Apache Kafka Producer and Consumer.

## Java Implementation

- includes code for both Consumer and Producer.

List of Maven dependencies included from mvnrepository

- kafka-clients
- slf4j-simple: Java abstraction for various logging frameworks<br/>(remove the scope tag to resolve error: "Falied to load class")
- slf4j-api
- jackson-databind

## Python Implementation

- includes code for Consumer

## Console Implementation

kafka.txt file contains the console commands for the following:

1. Starting zookeeper
2. Starting Kafka server
3. Creating topics
4. Listing the available topics
5. Adding messages to the topic through Producer
6. Using Consumer to read the messages from the partition.
