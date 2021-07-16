package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.csv.CSVRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

class SqlWriter{
    public static void writeToDb(String item){
        int i = item.lastIndexOf('[');
        String valuesStr = item.substring(i+1);
        valuesStr = valuesStr.substring(0,valuesStr.length()-2);

        String[] valuesList = valuesStr.split(", ");

        String year = valuesList[0];
        String brandName = valuesList[1];
        String genericName = valuesList[2];
        String coverageType = valuesList[3];
        Double totalSpending = Double.parseDouble(valuesList[4]);
        int sno = Integer.parseInt(valuesList[18]);

//        System.out.println(year+" "+brandName+" "+genericName+" "+coverageType+" "+totalSpending+" "+sno);

        String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        String DB_URL="jdbc:mysql://localhost:3306/test";

        // Database credentials
        String USER = "root";
        String PASS = "root";

        try {

            Class.forName("com.mysql.cj.jdbc.Driver");

            Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);


            PreparedStatement stmt=conn.prepareStatement("Insert into health_data values(?,?,?,?,?,?)");

            stmt.setInt(1,sno);
            stmt.setString(2,year);
            stmt.setString(3,brandName);
            stmt.setString(4,genericName);
            stmt.setString(5,coverageType);
            stmt.setDouble(6,totalSpending);

            int result = stmt.executeUpdate();
            if(result == 1){
                System.out.println("Insertion successful.");
            }

            conn.close();

        }catch(SQLException se) {
            se.printStackTrace();
        }catch(Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        }
    }
}

public class Consumer {
    public static void main(String[] args) {
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

    }
}
