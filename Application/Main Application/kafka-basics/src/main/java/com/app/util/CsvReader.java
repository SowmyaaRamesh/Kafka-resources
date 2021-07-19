package com.app.util;

import com.app.util.ExcelWriter;
import com.app.model.HealthData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class CsvReader {

    public static void readCsvFile(String filePath) throws IOException {
        Map<String, Object[]> excelData = new TreeMap<String, Object[]>();
        ErrorLogger errLogger = new ErrorLogger();
        List<String> brandNameList = new ArrayList<>();
        List<Integer> errorIndices = new ArrayList<>();

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
            errLogger.writeLogToFile("Successfully read csv file\n");

            errorIndices = Validator.validateBrandName(brandNameList);

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


                excelData.put(String.valueOf(key++), errObjList.toArray());
            }
            ExcelWriter.writeErrorDataToExcel(excelData);
        }
    }


}
