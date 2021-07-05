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

    public static void main(String[] args) throws IOException{

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter file path:");
        String filePath = scanner.nextLine();

        String extension= getFileExtension(filePath);
        System.out.println(extension);



        BufferedReader reader = null;
        String line;

//        try{
//            reader = new BufferedReader(new FileReader(filePath));
//            while((line = reader.readLine())!=null){
//                String[] row = line.split(",");
//                for(String index:row){
//                    System.out.printf("%-10s",index);
//                }
//                System.out.println();
//
//
//            }
//
//        }catch(Exception e){
//            e.printStackTrace();
//
//        }finally{
//            reader.close();
//
//        }

    }
}
