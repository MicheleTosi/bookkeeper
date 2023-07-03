package org.apache.bookkeeper.utils;

import java.io.File;
import java.io.IOException;

public class DeleteTempFiles {

    public static void deleteTempFiles(){
        try {
            File tempFile=File.createTempFile("bookie","");
            File tempDirPath=new File(tempFile.getParent());
            deleteFilesAndDirectoriesStartingWith(tempDirPath, "bookie");
            deleteFilesAndDirectoriesStartingWith(tempDirPath, "zookeeper");
        } catch (IOException e) {
            //
        }
    }

    private static void deleteFilesAndDirectoriesStartingWith(File directory, String prefix) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().startsWith(prefix)) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
        }
    }

    private static void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }
}
