package org.apache.bookkeeper.utils;

import org.apache.bookkeeper.test.TmpDirs;

import java.io.File;
import java.util.ArrayList;

public class DirsArrayBuilder {
    private final static TmpDirs tmpDirs=new TmpDirs();
    private final static ArrayList<File> fileList=new ArrayList<>();

    public static File[] getArrayWithAnInvalidDir(){
        return new File[]{new File("/home/bookie-impl-test")};
    }

    public static File[] getInvalidDirsArray(){
        return new File[]{new File("/home/bookie-impl-test"), new File("/home/bookie-impl-test2")};
    }

    public static File[] getArrayWithInvalidAndExistentValidDirs() throws Exception {
        tmpDirs.createNew("bookie-impl-test/bookie", null);
        return new File[]{new File("/home/bookie-impl-test"), new File("bookie-impl-test/bookie")};
    }

    public static File[] getArrayWithInvalidAndNotExistentValidDirs() throws Exception {
        return new File[]{new File("/home/bookie-impl-test"), new File("bookie-impl-test/bookie")};
    }

    public static File[] getInvalidAndNullDirs(){
        return new File[]{new File("/home/bookie-impl-test"), null};
    }

    public static File[] getFileAndInvalidDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        File file=new File(dir.getAbsolutePath()+"file.txt");
        boolean created = file.createNewFile();
        if (!created){
            throw new Exception("Errore nella creazione del file");
        }
        fileList.add(file);
        return new File[]{new File("/home/bookie-impl-test"), file};
    }

    public static File[] getArrayWithValidExistentDir() throws Exception {
        tmpDirs.createNew("bookie-impl-test/bookie", null);
        return new File[]{new File("bookie-impl-test/bookie")};
    }

    public static File[] getArrayValidExistentDirs() throws Exception {
        tmpDirs.createNew("bookie-impl-test/bookie", null);
        tmpDirs.createNew("bookie-impl-test/bookie2", null);
        return new File[]{new File("bookie-impl-test/bookie"),new File("bookie-impl-test/bookie2")};
    }

    public static File[] getArrayWithAValidExistentAndNotExistentDirs() throws Exception {
        tmpDirs.createNew("bookie-impl-test/bookie", null);
        return new File[]{new File("bookie-impl-test/bookie"),new File("bookie-impl-test/bookie2")};
    }

    public static File[] getArrayValidExistentAndNullDirs() throws Exception {
        tmpDirs.createNew("bookie-impl-test/bookie", null);
        return new File[]{new File("bookie-impl-test/bookie"),null};
    }

    public static File[] getFileAndValidExistentDirs() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        File file=new File(dir.getAbsolutePath()+"file.txt");
        boolean created = file.createNewFile();
        if (!created){
            throw new Exception("Errore nella creazione del file");
        }
        fileList.add(file);
        return new File[]{new File("bookie-impl-test/bookie"),file};
    }

    public static File[] getArrayWithValidDir() {
        return new File[]{new File("bookie-impl-test/bookie")};
    }

    public static File[] getArrayWithValidDirs() {
        return new File[]{new File("bookie-impl-test/bookie"), new File("bookie-impl-test/bookie2")};
    }

    public static File[] getArrayWithValidAndNullDirs() {
        return new File[]{new File("bookie-impl-test/bookie"), null};
    }

    public static File[] getFileAndValidDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        File file=new File(dir.getAbsolutePath()+"file.txt");
        boolean created = file.createNewFile();
        if (!created){
            throw new Exception("Errore nella creazione del file");
        }
        fileList.add(file);
        return new File[]{new File("bookie-impl-test/bookie"), file};
    }

    public static File[] getArrayWithAFile() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        File file=new File(dir.getAbsolutePath()+"file.txt");
        boolean created = file.createNewFile();
        if (!created){
            throw new Exception("Errore nella creazione del file");
        }
        fileList.add(file);
        return new File[]{file};
    }

    public static File[] getArrayWithFileAndNull() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        File file=new File(dir.getAbsolutePath()+"file.txt");
        boolean created = file.createNewFile();
        if (!created){
            throw new Exception("Errore nella creazione del file");
        }
        fileList.add(file);
        return new File[]{file, null};
    }

    public static File[] getArrayWithFiles() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        File file=new File(dir.getAbsolutePath()+"file.txt");
        boolean created = file.createNewFile();
        if (!created){
            throw new Exception("Errore nella creazione del file");
        }
        File dir2=tmpDirs.createNew("bookie-impl-test/bookie2", null);
        File file2=new File(dir2.getAbsolutePath()+"file.txt");
        created = file2.createNewFile();
        if (!created){
            throw new Exception("Errore nella creazione del file");
        }
        fileList.add(file);
        fileList.add(file2);
        return new File[]{file, file2};
    }

    public static void cleanup() throws Exception {
        tmpDirs.cleanup();
        for(File file:fileList){
            file.delete();
        }
    }

}
