package com.app.util;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class Validator {
    public static List<Integer> errorIndices = new ArrayList<>();

    public static List<Integer> validateBrandName(List<String> brandNameList) {
        ListIterator<String> brandNameItr = brandNameList.listIterator();
        ErrorLogger errLogger = new ErrorLogger();

        errLogger.writeLogToFile("Validating column: brand_name.\n");

        while (brandNameItr.hasNext()) {
            if (brandNameItr.next().equals("")) {
                errorIndices.add((brandNameItr.nextIndex() - 1));
                errLogger.writeLogToFile("[Error in row " + (brandNameItr.nextIndex() + 1) + "]: field should not be empty\n");
            }
        }
        return errorIndices;
    }

    public static List<Integer> getErrorIndices() {
        return errorIndices;
    }
}
