package com.example.kafka;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.*;

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
    public static void writeErrorDatatoExcel(Collection<String> errData) throws IOException{
        try{
            FileInputStream inputStream = new FileInputStream(new File("ErrorData.xlsx"));
            Workbook workbook = WorkbookFactory.create(inputStream);

            Sheet sheet = workbook.getSheetAt(0);
            int rowId = sheet.getLastRowNum();
            System.out.println("Rowid:"+rowId);

            // writing the data into the sheets...
            Row row = sheet.createRow(rowId);
            rowid++;

            int cellid = 0;

            for (Object obj : errData) {
                Cell cell = row.createCell(cellid++);
                cell.setCellValue((String)obj);
            }
            System.out.println("here");
            inputStream.close();
            FileOutputStream out = new FileOutputStream(
                    new File("ErrorData.xlsx"),true);
            workbook.write(out);
            out.close();

        }catch(Exception e){
            e.printStackTrace();
        }

//        // workbook object
//        XSSFWorkbook workbook = new XSSFWorkbook();
//
//        // spreadsheet object
//        XSSFSheet spreadsheet
//                = workbook.createSheet(" Student Data ");
//
//        // creating a row object
//        XSSFRow row;



//        if(rowid == 1){
//            FileOutputStream out = new FileOutputStream(
//                    new File("ErrorData.xlsx"));
//            workbook.write(out);
//            out.close();
//        } else{

//        }




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

//        int[] errorIndexList=new int[200];
//        int index = 0;


        List<Integer> errorIndexList = new ArrayList<>();

        while(brandNameItr.hasNext()){
            if(brandNameItr.next().equals("")){
//                System.out.println(brandNameItr.nextIndex());
                errorIndexList.add((brandNameItr.nextIndex()-1));

//                System.out.println("Error in row "+(brandNameItr.nextIndex()+1) + ": field should not be empty");
                ErrorLogger.writeLogToFile("[Error in row "+(brandNameItr.nextIndex()+1) + "]: field should not be empty\n");
            }
        }

        return errorIndexList;

    }
    public static void csvReader(String filePath) throws IOException {

        List<String> brandNameList = new ArrayList<>();
        List<Integer> errorIndices = new ArrayList<>();

        try (Reader reader = new FileReader(filePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {
            System.out.println(csvParser.getHeaderNames());
            ExcelWriter.writeErrorDatatoExcel(csvParser.getHeaderNames());
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

            while(indexItr.hasNext()){
                index = indexItr.next();
                while (index!= itr.nextIndex() && itr.hasNext()){
                    CSVRecord data = itr.next();
                    continue;
                }
                CSVRecord errRecord = itr.next();
                System.out.println(errRecord.toMap().values());
//                ExcelWriter.writeErrorDatatoExcel(errRecord.toMap().values());
            }



//          System.out.println(itr.next()); //Output: CSVRecord [comment='null', recordNumber=1, values=[2010, Abilify, Aripiprazole, Part D, 1225884701, 338626, .....]]
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


