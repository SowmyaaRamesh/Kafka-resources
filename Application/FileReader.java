package com.example.kafka;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

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
//            System.out.printf("i,j:%d,%d\n",i,columnCount);
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

    public static void main(String[] args) throws IOException {

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter file path:");
        String filePath = scanner.nextLine();

        String extension= getFileExtension(filePath);
//        System.out.println(extension);

        switch(extension){
            case "csv": csvReader(filePath);
                        break;
            default: System.out.println("Invalid file type");
        }

    }
}


//C:\\Users\\Sowmya\\Downloads\\health-data.csv


