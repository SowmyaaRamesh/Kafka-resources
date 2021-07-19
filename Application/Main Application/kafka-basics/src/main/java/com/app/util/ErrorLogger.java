package com.app.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class ErrorLogger {
    public void writeLogToFile(String message){
        try{
            BufferedWriter out = new BufferedWriter(new FileWriter("log.txt", true));
            out.write(message);
            out.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}