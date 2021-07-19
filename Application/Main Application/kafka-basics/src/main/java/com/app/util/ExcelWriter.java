package com.app.util;


import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class ExcelWriter{
    static int rowid=0;
    public static void writeErrorDataToExcel(Map<String,Object[]> errData) throws IOException {


        // workbook object
        XSSFWorkbook workbook = new XSSFWorkbook();

        // spreadsheet object
        XSSFSheet spreadsheet = workbook.createSheet(" Student Data ");

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