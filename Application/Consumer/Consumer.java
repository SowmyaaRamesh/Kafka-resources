import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

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
        String item;
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
                item = record.value().toString();
                SqlWriter.writeToDb(item);
            }

        }

//        SqlWriter.closeDatabaseConnection();

//        String item = "CSVRecord [comment='null', recordNumber=27, values=[2014, Advair Diskus, Fluticasone/Salmeterol, Part D, 2276374749, 1420748, 460372139, 211936509.1, 1602.24, 4.94, , 6094244, 668600, 752148, 12718030.25, 199218478.9, 19.02, 264.87, 27]]";
//        SqlWriter.writeToDb(item);
//        item ="CSVRecord [comment='null', recordNumber=29, values=[2011, Afinitor, Everolimus, Part D, 45140530.91, 1573, 179682, 2290729.94, 28697.1, 251.58, , 6076, 620, 953, 36885.89, 2253844.05, 59.49, 2365, 29]]";
//        SqlWriter.writeToDb(item);

    }
}
