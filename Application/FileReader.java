package com.example.kafka;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

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
/*
    public static void csvReader(String filePath) throws IOException{
        BufferedReader reader = null;
        String line;
        String[][] fileContents = new String[580][20];
        int i=0,j=0;
        int columnCount = 0;

        try{
            reader = new BufferedReader(new FileReader(filePath));
            while((line = reader.readLine())!=null){
                String[] row = line.split(",");
                for(String value:row){
                    fileContents[i][j++] = value;
                }

                if(i==0){
                    columnCount = j;
                }
                i++;
                j=0;
            }
            System.out.printf("i,j:%d,%d\n",i,columnCount);
            for(int x=0;x<i;x++){
                for(int y=0;y<columnCount;y++){
                    System.out.printf("%-28s",fileContents[x][y]);
                }
                System.out.println();
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            reader.close();
        }
    }
*/

    public static void validateBrandName(List<String> brandNameList){
        ListIterator<String> brandNameItr = brandNameList.listIterator();
        com.example.kafka.ErrorLogger.writeLogToFile("Validating column: brand_name.\n");

        while(brandNameItr.hasNext()){
            if(brandNameItr.next().equals("")){
                System.out.println("Error in row "+(brandNameItr.nextIndex()+1) + ": field should not be empty");
                com.example.kafka.ErrorLogger.writeLogToFile("[Error in row "+(brandNameItr.nextIndex()+1) + "]: field should not be empty\n");
            }
        }


    }
    public static void csvReader(String filePath) throws IOException {

        List<String> brandNameList = new ArrayList<>();

        try (Reader reader = new FileReader(filePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {
            System.out.println(csvParser.getHeaderNames());

            System.out.println("Year " + "\t\t" + "Brand Name " + "\t\t\t" + "Generic Name"+ "\t\t\t\t\t\t" +"Coverage Type" + "\t\t\t" + "Total Spending");
            for (CSVRecord record : csvParser) {
//                System.out.print(csvParser.getCurrentLineNumber());

                String brandName = record.get("brand_name");
                brandNameList.add(brandName);
                String year = record.get("year");
                String genericName = record.get("generic_name");
                String coverageType = record.get("coverage_type");
                String totalSpending = record.get("total_spending");

//                System.out.printf("%-15s%-24s%-30s%-20s%-20s\n", year, brandName, genericName, coverageType, totalSpending );
            }
//            System.out.println(brandNameList);
            com.example.kafka.ErrorLogger.writeLogToFile("Successfully read csv file\n");
            validateBrandName(brandNameList);

            List<CSVRecord> records = new ArrayList<>();
            records = csvParser.getRecords();
            Iterator<CSVRecord> itr = records.iterator();

//            System.out.println(itr.next()); //Output: CSVRecord [comment='null', recordNumber=1, values=[2010, Abilify, Aripiprazole, Part D, 1225884701, 338626, .....]]
        }
    }

    public static void main(String[] args) throws IOException {

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter file path:");
        String filePath = scanner.nextLine();

        String extension= getFileExtension(filePath);
//        System.out.println(extension);
        com.example.kafka.ErrorLogger.writeLogToFile("Input file found to be of type:"+extension+"\n");



        switch(extension){
            case "csv": csvReader(filePath);
                break;
            default: System.out.println("Invalid file type");
                com.example.kafka.ErrorLogger.writeLogToFile("Invalid file type:"+extension+"\n");
        }

    }
}


//C:\\Users\\Sowmya\\Downloads\\health-data.csv


