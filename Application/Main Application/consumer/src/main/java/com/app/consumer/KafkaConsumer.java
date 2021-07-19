package com.app.consumer;

import com.app.dao.HealthDataDAO;
import com.app.model.HealthData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import com.app.util.Convertor;

public class KafkaConsumer {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

        // create variables for strings
        final String bootstrapServers = "127.0.0.1:9092";
        final String consumerGroupID = "java-group-consumer";

        // Create properties object for Consumer
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        //This might be a new group, has to consume for first time so specify offset to start consuming the records
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer
        final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(prop);

        //subscribe to topics
        consumer.subscribe(Arrays.asList("Test"));


        HealthDataDAO dao = new HealthDataDAO();
        //poll and consume the Records
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : records) {
                logger.info("Received new record: \n" +
                        "Key: " + record.key() + ", " +
                        "Value: " + record.value() + ", " +
                        "Topic: " + record.topic() + ", " +
                        "Partition: " + record.partition() + ", " +
                        "Offset: " + record.offset() + "\n");
                HealthData item = Convertor.convertJsonToHealthDataObj(record.value().toString());
//                ArrayList<String> dbRecords = new ArrayList<String>();
//                dbRecords.add(item.getYear());
//                dbRecords.add(item.getBrandName());
//                dbRecords.add(item.getGenericName());
//                dbRecords.add(item.getCoverageType());
//                dbRecords.add(item.getTotalSpending());
//                dbRecords.add(item.getSno());
////                System.out.println(dbRecords);
                dao.addRecords(item);


            }

        }

    }
}