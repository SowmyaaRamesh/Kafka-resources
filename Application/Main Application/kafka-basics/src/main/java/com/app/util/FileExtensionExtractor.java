package com.app.util;

public class FileExtensionExtractor {
    /**
     * Returns the extension of the file from the file path
     *
     * @param filePath The file path input by the user
     */
    public String getFileExtension(String filePath) {

        String extension = "";
        int i = filePath.lastIndexOf('.');
        if (i > 0) {
            extension = filePath.substring(i + 1);
        }
        return extension;
    }
}
