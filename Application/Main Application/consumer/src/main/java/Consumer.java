import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


import com.app.model.HealthData;

import static com.app.util.Convertor.convertJsonToHealthDataObj;

public class Consumer {

    public static void main(String[] args){
        final Logger logger = LoggerFactory.getLogger(Consumer.class);

        // create variables for strings
        final String bootstrapServers = "127.0.0.1:9092";
        final String consumerGroupID = "java-group-consumer";

        // Create properties object for Consumer
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,consumerGroupID);
        //This might be a new group, has to consume for first time so specify offset to start consuming the records
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        final KafkaConsumer<String,String> consumer = new KafkaConsumer<>(prop);

        //subscribe to topics
        consumer.subscribe(Arrays.asList("Test"));
        HealthData item;
        SqlWriter.setUpDatabaseConnection();

        //poll and consume the Records
        while(true) {

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : records) {
                logger.info("Received new record: \n"+
                        "Key: "+ record.key()+", "+
                        "Value: "+record.value()+", "+
                        "Topic: "+record.topic()+", "+
                        "Partition: "+record.partition()+", "+
                        "Offset: "+record.offset()+"\n");
                item = convertJsonToHealthDataObj(record.value().toString());
//                SqlWriter.writeToDb(dbRecords);
            }

        }

//        SqlWriter.closeDatabaseConnection();

//{"year":"2015","brandName":"Zytiga","genericName":"Abiraterone Acetate","coverageType":"Part D","totalSpending":"790049711.1"}
    }
}
