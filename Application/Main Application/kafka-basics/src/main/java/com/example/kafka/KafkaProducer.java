//package com.example.kafka;
//
//
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.JsonSerializer;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVParser;
//import org.apache.commons.csv.CSVRecord;
//
//import org.apache.poi.ss.usermodel.*;
//import org.apache.poi.xssf.usermodel.XSSFRow;
//import org.apache.poi.xssf.usermodel.XSSFSheet;
//import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//
//import java.io.*;
//import java.util.*;
//
//import org.apache.kafka.clients.producer.*;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Properties;
//
//
//
//
//
//public class SampleProducer {
//
//    public static void producerCode(ArrayList<String> list) {
//        final Logger logger = LoggerFactory.getLogger(SampleProducer.class);
//
//        // Create properties object for Producer
//        Properties prop = new Properties();
//        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        //Create the Producer
//        final KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
//        Iterator<String> validatedRecordListItr = list.iterator();
//
//        for(int i=0;i<list.size();i++){
////             Create the ProducerRecord
//
//            ProducerRecord<String,String> record = new ProducerRecord<String,String>("HealthData", validatedRecordListItr.next());
//
//            //Send Data - Asynchronous
//
//            producer.send(record, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if(e == null){
//                        // success case
//                        logger.info("\nReceived record metadata. \n"+
//                                "Topic:" + recordMetadata.topic()+", Partition:"+ recordMetadata.partition()+","+
//                                "Offset:" + recordMetadata.offset()+" @ Timestamp: "+ recordMetadata.timestamp()+"\n");
//
//                    } else {
//                        logger.error("Error while writing to cluster:", e);
//                    }
//                }
//            });
//
//        }
//
//        //flush and close producer
//        producer.flush(); //writes any pending data into the topic before closing
//        producer.close();
//    }
//
//
//
//    public static void main(String[] args) throws IOException {
//
//    }
//}
//
////file path:C:\Users\mkatari3\Downloads\health-data.csv