package org.apache.bookkeeper.utils;

import org.apache.bookkeeper.test.TmpDirs;
import org.apache.bookkeeper.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class DirsArrayBuilder {
    private final TmpDirs tmpDirs=new TmpDirs();
    public DirsArrayBuilder(){

    }

    public File[] getArrayWithAnInvalidDir(){
        return new File[]{new File("/home/bookie-impl-test")};
    }

    public File[] getInvalidDirsArray(){
        return new File[]{new File("/home/bookie-impl-test"), new File("/home/bookie-impl-test2")};
    }

    public File[] getArrayWithInvalidAndExistentValidDirs() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        return new File[]{new File("/home/bookie-impl-test"), dir};
    }

    public File[] getArrayWithInvalidAndNotExistentValidDirs() throws Exception {
        return new File[]{new File("/home/bookie-impl-test"), new File("/tmp/bookie-impl-test/bookie")};
    }

    public File[] getInvalidAndNullDirs(){
        return new File[]{new File("/home/bookie-impl-test"), null};
    }

    public File[] getFileAndInvalidDir() throws Exception {
        File dir=tmpDirs.createNew("bookie", null);
        return new File[]{new File("/home/bookie-impl-test"), createNewFileInDir(dir, "bookie-impl")};
    }

    public File[] getArrayWithValidExistentDir() throws Exception {
        File dir=tmpDirs.createNew("bookie", null);
        createNewFileInDir(dir, "bookie");
        return new File[]{dir};
    }

    public File[] getArrayValidExistentDirs() throws Exception {
        File dir=tmpDirs.createNew("bookie", null);
        File dir2=tmpDirs.createNew("bookie2", null);
        createNewDirInDir(dir, "bookie-impl");
        createNewFileInDir(dir, "bookie-impl");
        return new File[]{dir, dir2};
    }

    public File[] getArrayWithAValidExistentAndNotExistentDirs() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        return new File[]{dir,new File("/tmp/bookie-impl-test/bookie2")};
    }

    public File[] getArrayValidExistentAndNullDirs() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        createNewFileInDir(dir,"bookie-impl");
        return new File[]{dir,null};
    }

    public File[] getFileAndValidExistentDirs() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        return new File[]{dir,File.createTempFile("bookie", ".tmp")};
    }

    public File[] getArrayWithValidDir() {
        return new File[]{new File("/tmp/bookie-impl-test/bookie")};
    }

    public File[] getArrayWithValidDirs() {
        return new File[]{new File("/tmp/bookie-impl-test/bookie"), new File("/tmp/bookie-impl-test/bookie2")};
    }

    public File[] getArrayWithValidAndNullDirs() {
        return new File[]{new File("/tmp/bookie-impl-test/bookie"), null};
    }

    public File[] getFileAndValidDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test/bookie", null);
        return new File[]{new File(dir.getParent()+"/bookie-impl-test/bookie2"), File.createTempFile("bookie", ".tmp")};
    }

    public File[] getArrayWithAFile() throws Exception {
        return new File[]{File.createTempFile("bookie", ".tmp")};
    }

    public File[] getArrayWithFileAndNull() throws Exception {
        return new File[]{File.createTempFile("bookie", ".tmp"), null};
    }

    public File[] getArrayWithFiles() throws Exception {
        return new File[]{File.createTempFile("bookie", ".tmp"),File.createTempFile("bookie2", ".tmp")};
    }

    private File createNewFileInDir(File dir, String fileName) throws IOException {
            Path innerDir=dir.toPath().resolve(fileName);
            File file=new File(innerDir.toUri());
            if(!file.createNewFile()){
                throw new IOException("Errore nella creazione del file");
            }
            return file;
    }

    private File createNewDirInDir(File dir, String fileName) throws IOException {
            return IOUtils.createTempDir(fileName, "", dir);
    }

}