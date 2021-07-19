package com.app.util;

import com.app.model.HealthData;
import com.app.producer.KafkaProducer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Scanner;

import com.app.util.CsvReader;

import com.app.util.Convertor;

public class DataTransformer {
    public static ArrayList<CSVRecord> getValidatedRecords(String filePath, List<Integer> errorIndices) throws FileNotFoundException {

        ArrayList<CSVRecord> validatedRecordList = new ArrayList<CSVRecord>();

        try (Reader reader = new FileReader(filePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {
            List<CSVRecord> records = csvParser.getRecords();
            ListIterator<CSVRecord> itr = records.listIterator();
            while (itr.hasNext()) {
                if (!errorIndices.contains(itr.nextIndex())) {
                    validatedRecordList.add(itr.next());
                } else {
                    itr.next();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return validatedRecordList;
    }

    public static ArrayList<String> getKafkaData(ArrayList<CSVRecord> validatedRecordList) {
        // Converting csv data to Json string
        ArrayList<String> jsonKafkaData = new ArrayList<String>();
        String jsonString = "";
        for (CSVRecord record : validatedRecordList) {
            HealthData data = new HealthData(record.get("year"), record.get("brand_name"), record.get("generic_name"), record.get("coverage_type"), record.get("total_spending"), record.get("serialid"));
            jsonString = Convertor.convertCsvRecordToJsonString(data);
            jsonKafkaData.add(jsonString);

        }
        return jsonKafkaData;
    }

    public static void main(String[] args) throws IOException {

        FileExtensionExtractor extnExtractor = new FileExtensionExtractor();
        ErrorLogger errLogger = new ErrorLogger();

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter file path:");
        String filePath = scanner.nextLine();


        String extension = extnExtractor.getFileExtension(filePath);
        errLogger.writeLogToFile("Input file found to be of type:" + extension + "\n");

        List<Integer> errorIndices;


        switch (extension) {
            case "csv":
                CsvReader.readCsvFile(filePath);
                errorIndices = Validator.getErrorIndices();
                ArrayList<CSVRecord> validatedRecordList = getValidatedRecords(filePath, errorIndices);
                ArrayList<String> jsonKafkaData = getKafkaData(validatedRecordList);
                KafkaProducer.writeToKafkaTopic(jsonKafkaData);
                break;
            default:
                System.out.println("Invalid file type");
                errLogger.writeLogToFile("Invalid file type:" + extension + "\n");
        }


    }

}

//C:\\Users\\Sowmya\\Downloads\\health-data.csv