package com.example.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class HealthData {
    private String year;
    private String brandName;
    private String genericName;
    private String coverageType;
    private String totalSpending;
    private String sno;

    public HealthData(String year, String brandName, String genericName, String coverageType, String totalSpending, String sno) {
        this.year = year;
        this.brandName = brandName;
        this.genericName = genericName;
        this.coverageType = coverageType;
        this.totalSpending = totalSpending;
        this.sno = sno;
    }
    public String getBrandName() {
        return brandName;
    }
    public String getYear() {
        return year;
    }
    public String getGenericName() {
        return genericName;
    }
    public String getCoverageType(){
        return coverageType;
    }
    public String getTotalSpending(){
        return totalSpending;
    }
    public String getSno(){
        return sno;
    }


}

class ErrorLogger {
    public static void writeLogToFile(String message){
        try{
            BufferedWriter out = new BufferedWriter(new FileWriter("log.txt", true));
            out.write(message);
            out.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}

class ExcelWriter{
    static int rowid=0;
    public static void writeErrorDataToExcel(Map<String,Object[]> errData) throws IOException{


        // workbook object
        XSSFWorkbook workbook = new XSSFWorkbook();

        // spreadsheet object
        XSSFSheet spreadsheet
                = workbook.createSheet(" Student Data ");

        // creating a row object
        XSSFRow row;

        // writing the data into the sheets...
        row = spreadsheet.createRow(rowid);
//        rowid++;
        Set<String> keyid = errData.keySet();

        int rowid = 0;

        // writing the data into the sheets...

        for (String key : keyid) {

            row = spreadsheet.createRow(rowid++);
            Object[] objectArr = errData.get(key);


            int cellid = 0;

            for (Object obj : objectArr) {
                Cell cell = row.createCell(cellid++);
                cell.setCellValue((String)obj);
            }
        }


        FileOutputStream out = new FileOutputStream(
                new File("ErrorData.xlsx"));
        workbook.write(out);
        out.close();

    }


}

public class SampleProducer {
    /**
     * Returns the extension of the file from the file path
     * @param  filePath The file path input by the user
     */
    public static String getFileExtension(String filePath){

        String extension="";
        int i = filePath.lastIndexOf('.');
        if (i > 0) {
            extension = filePath.substring(i+1);
        }
        return extension;
    }


    public static List<Integer> validateBrandName(List<String> brandNameList){
        ListIterator<String> brandNameItr = brandNameList.listIterator();

        ErrorLogger.writeLogToFile("Validating column: brand_name.\n");

        List<Integer> errorIndexList = new ArrayList<>();

        while(brandNameItr.hasNext()){
            if(brandNameItr.next().equals("")){
                errorIndexList.add((brandNameItr.nextIndex()-1));
                ErrorLogger.writeLogToFile("[Error in row "+(brandNameItr.nextIndex()+1) + "]: field should not be empty\n");
            }
        }

        return errorIndexList;

    }
    public static String convertCsvRecordToJsonString(HealthData record) {
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = "";

        try {
            jsonString = mapper.writeValueAsString(record);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return jsonString;
    }
    public static void csvReader(String filePath) throws IOException {

        List<String> brandNameList = new ArrayList<>();
        List<Integer> errorIndices = new ArrayList<>();
        Map<String, Object[]> excelData = new TreeMap<String, Object[]>();
        int key = 0;
        int noOfColumns;

        try (Reader reader = new FileReader(filePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {
            noOfColumns = csvParser.getHeaderNames().size();
            ArrayList<String> headerNames = new ArrayList<String>();
//            for(String name:csvParser.getHeaderNames()){
//                headerNames.add(name);
//            }
            headerNames.add("year");
            headerNames.add("brand_name");
            headerNames.add("generic_name");
            headerNames.add("coverage_type");
            headerNames.add("row_number");
            headerNames.add("error_message");

            Object[] headerArray = headerNames.toArray();


            excelData.put(String.valueOf(key++),headerArray);

            for (CSVRecord record : csvParser) {
                String brandName = record.get("brand_name");
                brandNameList.add(brandName);
            }
            ErrorLogger.writeLogToFile("Successfully read csv file\n");

            errorIndices = validateBrandName(brandNameList);


        }

        try (Reader reader = new FileReader(filePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {

            List<CSVRecord> records = csvParser.getRecords();

            ListIterator<CSVRecord> itr = records.listIterator();

            ListIterator<Integer> indexItr = errorIndices.listIterator();


            int index;


            while (indexItr.hasNext()) {
                index = indexItr.next();
                while (index != itr.nextIndex() && itr.hasNext()) {
                    CSVRecord data = itr.next();
                    continue;
                }
                CSVRecord errRecord = itr.next();

                ArrayList<String> errObjList = new ArrayList<>();

                String rowNo = String.valueOf(index + 2);
                String year = errRecord.get(0);
                String brand = errRecord.get(1);
                String generic = errRecord.get(2);
                String coverage = errRecord.get(3);
                String errorMessage = "brand_name cannot be empty or null.";
                errObjList.add(year);
                errObjList.add(brand);
                errObjList.add(generic);
                errObjList.add(coverage);
                errObjList.add(rowNo);
                errObjList.add(errorMessage);


//                System.out.println(errObjList);
                excelData.put(String.valueOf(key++), errObjList.toArray());

            }

            ExcelWriter.writeErrorDataToExcel(excelData);
        }

        try (Reader reader = new FileReader(filePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {

            List<CSVRecord> records = csvParser.getRecords();

            ListIterator<CSVRecord> itr = records.listIterator();


//            Write validated productions to Kafka


            ArrayList<CSVRecord> validatedRecordList = new ArrayList<CSVRecord>();

            while (itr.hasNext()) {
                if (!errorIndices.contains(itr.nextIndex())) {
                    validatedRecordList.add(itr.next());
                }else{
                    itr.next();
                }

            }
//            System.out.println(indices);
//            System.out.println(validatedRecordList);
//            Iterator<CSVRecord> validatedRecordListItr = validatedRecordList.iterator();
//            while(validatedRecordListItr.hasNext()){
//                String jsonString = convertCsvRecordToJsonString(validatedRecordListItr.next());
//                System.out.println(jsonString);
//            }

            String jsonString="";
            for(CSVRecord record:validatedRecordList){
                HealthData data = new HealthData(record.get("year"),record.get("brand_name"),record.get("generic_name"),record.get("coverage_type"),record.get("total_spending"),record.get("serialid"));
                jsonString = convertCsvRecordToJsonString(data);

            }



            System.out.println(jsonString);
            final Logger logger = LoggerFactory.getLogger(Producer.class);

            // Create properties object for Producer
            Properties prop = new Properties();
            prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


            //Create the Producer
            final KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
            Iterator<CSVRecord> validatedRecordListItr = validatedRecordList.iterator();

//            for(int i=0;i<3;i++){
////                 Create the ProducerRecord


                ProducerRecord<String,String> record = new ProducerRecord<String,String>("Test",jsonString);

                //Send Data - Asynchronous

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null){
                            // success case
                            logger.info("\nReceived record metadata. \n"+
                                    "Topic:" + recordMetadata.topic()+", Partition:"+ recordMetadata.partition()+","+
                                    "Offset:" + recordMetadata.offset()+" @ Timestamp: "+ recordMetadata.timestamp()+"\n");

                        } else {
                            logger.error("Error while writing to cluster:", e);
                        }
                    }
                });

//            }

            //flush and close producer
            producer.flush(); //writes any pending data into the topic before closing
            producer.close();

        }
    }

    public static void main(String[] args) throws IOException {

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter file path:");
        String filePath = scanner.nextLine();

        String extension= getFileExtension(filePath);
        ErrorLogger.writeLogToFile("Input file found to be of type:"+extension+"\n");


        switch(extension){
            case "csv": csvReader(filePath);
                        break;
            default: System.out.println("Invalid file type");
                     ErrorLogger.writeLogToFile("Invalid file type:"+extension+"\n");
        }

    }
}


//C:\\Users\\Sowmya\\Downloads\\health-data.csv


