package com.app.producer;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducer {
    public static void writeToKafkaTopic(ArrayList<String> kafkaData) {

        final Logger logger = LoggerFactory.getLogger(Producer.class);

        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(prop);
        Iterator<String> kafkaDataItr = kafkaData.iterator();

        for (int i = 0; i < 5; i++) {

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("Test", kafkaDataItr.next());


            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        // success case
                        logger.info("\nReceived record metadata. \n" +
                                "Topic:" + recordMetadata.topic() + ", Partition:" + recordMetadata.partition() + "," +
                                "Offset:" + recordMetadata.offset() + " @ Timestamp: " + recordMetadata.timestamp() + "\n");

                    } else {
                        logger.error("Error while writing to cluster:", e);
                    }
                }
            });

        }


        producer.flush();
        producer.close();

    }


}
