package org.apache.bookkeeper.utils;

import org.apache.bookkeeper.bookie.DirArrayEnum;
import org.apache.bookkeeper.test.TmpDirs;
import org.apache.bookkeeper.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.mockito.Mockito.*;

public class DirsArrayBuilder {
    private final static TmpDirs tmpDirs=new TmpDirs();

    private static File[] getArrayWithFile() throws Exception {
        return new File[]{File.createTempFile("bookie", ".tmp")};
    }

    private static File[] getArrayWithFileAndNull() throws Exception {
        return new File[]{File.createTempFile("bookie", ".tmp"), null};
    }

    private static File[] getArrayWithFiles() throws Exception {
        return new File[]{File.createTempFile("bookie", ".tmp"),File.createTempFile("bookie2", ".tmp")};
    }

    private static File createNewFileInDir(File dir, String fileName) throws IOException {
            Path innerDir=dir.toPath().resolve(fileName);
            File file=new File(innerDir.toUri());
            if(!file.createNewFile()){
                throw new IOException("Errore nella creazione del file");
            }
            return file;
    }

    private static File createNewDirInDir(File dir, String fileName) throws IOException {
            return IOUtils.createTempDir(fileName, "", dir);
    }

    private static File createNotRemovableDir() throws IOException {
        File dir=mock(File.class);
        when(dir.delete()).thenReturn(false);
        when(dir.isDirectory()).thenReturn(true);
        when(dir.exists()).thenReturn(true);
        return dir;
    }

    private static File createNotRemovableDirContainingDir() throws IOException {
        File dir=createNotRemovableDir();
        File dir1=mock(File.class);
        when(dir1.delete()).thenReturn(false);
        when(dir1.isDirectory()).thenReturn(true);
        when(dir1.exists()).thenReturn(true);
        when(dir.listFiles()).thenReturn(new File[]{dir1});
        return dir;
    }

    private static File createNotRemovableDirContainingFile() throws IOException {
        File dir=createNotRemovableDir();
        File file1=mock(File.class);
        when(file1.delete()).thenReturn(false);
        when(file1.isDirectory()).thenReturn(false);
        when(file1.exists()).thenReturn(true);
        when(dir.listFiles()).thenReturn(new File[]{file1});
        return dir;
    }

    private static File[] getArrayWithNullAndExistentRemovableEmptyDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        return new File[]{null, dir};
    }

    private static File[] getArrayWithNullAndExistentRemovableDirContainingDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir, "test");
        return new File[]{null, dir};
    }

    private static File[] getArrayWithNullAndExistentRemovableDirContainingFile() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir, "test");
        return new File[]{null, dir};
    }

    private static File[] getArrayWithNullAndExistentNotRemovableEmptyDir()  throws Exception {
        return new File[]{null, createNotRemovableDir()};
    }

    private static File[] getArrayWithNullAndExistentNotRemovableDirContainingDir() throws IOException {
        return new File[]{null, createNotRemovableDirContainingDir()};
    }

    private static File[] getArrayWithNullAndExistentNotRemovableDirContainingFile() throws IOException {
        return new File[]{null, createNotRemovableDirContainingFile()};
    }

    private static File[] getArrayWithNullAndNotExistentWithoutPermissionDir() {
        return new File[]{null, new File("/home/bookie")};
    }

    private static File[] getArrayWithNullAndNotExistentWithPermissionDir() {
        return new File[]{null, new File("/tmp/bookie-impl-test")};
    }

    private static File[] getArrayWithFileAndExistentRemovableEmptyDir() throws Exception {
        return new File[]{File.createTempFile("bookie-impl-test-file", null), tmpDirs.createNew("bookie-impl-test", null)};
    }

    private static File[] getArrayWithFileAndExistentRemovableDirContainingDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir, "test");
        return new File[]{File.createTempFile("bookie-impl-test-file", null), dir};
    }

    private static File[] getArrayWithFileAndExistentRemovableDirContainingFile() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir, "test.txt");
        return new File[]{File.createTempFile("bookie-impl-test-file", null), dir};
    }

    private static File[] getArrayWithFileAndExistentNotRemovableEmptyDir() throws Exception {
        return new File[]{File.createTempFile("bookie-impl-test-file", null), createNotRemovableDir()};
    }

    private static File[] getArrayWithFileAndExistentNotRemovableDirContainingDir() throws Exception {
        return new File[]{File.createTempFile("bookie-impl-test-file", null), createNotRemovableDirContainingDir()};
    }

    private static File[] getArrayWithFileAndExistentNotRemovableDirContainingFile() throws Exception {
        return new File[]{File.createTempFile("bookie-impl-test-file", null), createNotRemovableDirContainingFile()};
    }

    private static File[] getArrayWithFileAndNotExistentWithoutPermissionDir() throws Exception {
        return new File[]{File.createTempFile("bookie-impl-test-file", null), new File("/home/bookie-test")};
    }

    private static File[] getArrayWithFileAndNotExistentWithPermissionDir() throws Exception {
        return new File[]{File.createTempFile("bookie-impl-test-file", null), new File("/tmp/bookie-test")};
    }


    private static File[] getArrayWithExistentRemovableEmptyDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        return new File[]{dir};
    }

    private static File[] getArrayWithTwoExistentRemovableEmptyDirs() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        File dir2=tmpDirs.createNew("bookie-impl-test", null);
        return new File[]{dir, dir2};
    }

    private static File[] getArrayWithExistentRemovableEmptyDirAndExistentRemovableDirContainingDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        File dir2=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir2, "test");
        return new File[]{dir, dir2};
    }

    private static File[] getArrayWithExistentRemovableEmptyDirAndExistentRemovableDirContainingFile() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        File dir2=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir2, "test.txt");
        return new File[]{dir, dir2};
    }

    private static File[] getArrayWithExistentRemovableEmptyDirAndExistentNotRemovableEmptyDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        return new File[]{dir, createNotRemovableDir()};
    }

    private static File[] getArrayWithExistentRemovableEmptyDirAndExistentNotRemovableDirContainingDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        return new File[]{dir, createNotRemovableDirContainingDir()};
    }

    private static File[] getArrayWithExistentRemovableEmptyDirAndExistentNotRemovableDirContainingFile() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        return new File[]{dir, createNotRemovableDirContainingFile()};
    }

    private static File[] getArrayWithExistentRemovableEmptyDirAndNotExistentWithoutPermissionDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        return new File[]{dir, new File("/home/bookie-impl")};
    }

    private static File[] getArrayWithExistentRemovableEmptyDirAndNotExistentWithPermissionDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        return new File[]{dir, new File("/tmp/bookie-impl")};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir, "test");
        return new File[]{dir};
    }

    private static File[] getArrayWithTwoExistentRemovableDirsContainingDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir, "test");
        File dir2=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir2, "test");
        return new File[]{dir, dir2};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingDirAndExistentRemovableDirContainingFile() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir, "test");
        File dir2=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir2, "test.txt");
        return new File[]{dir, dir2};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingDirAndExistentNotRemovableEmptyDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir, "test");
        return new File[]{dir, createNotRemovableDir()};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingDirAndExistentNotRemovableDirContainingDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir, "test");
        return new File[]{dir, createNotRemovableDirContainingDir()};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingDirAndExistentNotRemovableDirContainingFile() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir, "test");
        return new File[]{dir, createNotRemovableDirContainingFile()};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingDirAndNotExistentWithoutPermissionDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir, "test");
        return new File[]{dir, new File("/home/bookie-impl")};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingDirAndNotExistentWithPermissionDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewDirInDir(dir, "test");
        return new File[]{dir, new File("/tmp/bookie-impl")};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingFile() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir, "test");
        return new File[]{dir};
    }

    private static File[] getArrayWithTwoExistentRemovableDirsContainingFile() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir, "test");
        File dir2=tmpDirs.createNew("bookie-impl-test2", null);
        createNewFileInDir(dir2, "test2");
        return new File[]{dir, dir2};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingFileAndExistentNotRemovableEmptyDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir, "test");
        return new File[]{dir, createNotRemovableDir()};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingFileAndExistentNotRemovableDirContainingDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir, "test");
        return new File[]{dir, createNotRemovableDirContainingDir()};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingFileAndExistentNotRemovableDirContainingFile() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir, "test");
        return new File[]{dir, createNotRemovableDirContainingFile()};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingFileAndNotExistentWithoutPermissionDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir, "test");
        return new File[]{dir, new File("/home/bookie-impl")};
    }

    private static File[] getArrayWithAnExistentRemovableDirContainingFileAndNotExistentWithPermissionDir() throws Exception {
        File dir=tmpDirs.createNew("bookie-impl-test", null);
        createNewFileInDir(dir, "test");
        return new File[]{dir, new File("/tmp/bookie-impl")};
    }

    private static File[] getArrayWithExistentNotRemovableEmptyDir() throws IOException {
        return new File[]{createNotRemovableDir()};
    }

    private static File[] getArrayWithTwoExistentNotRemovableEmptyDirs() throws IOException {
        return new File[]{createNotRemovableDir(), createNotRemovableDir()};
    }

    private static File[] getArrayWithExistentNotRemovableEmptyDirAndExistentNotRemovableDirContainingDir() throws IOException {
        return new File[]{createNotRemovableDir(), createNotRemovableDirContainingDir()};
    }

    private static File[] getArrayWithExistentNotRemovableEmptyDirAndExistentNotRemovableDirContainingFile() throws IOException {
        return new File[]{createNotRemovableDir(), createNotRemovableDirContainingFile()};
    }

    private static File[] getArrayWithExistentNotRemovableEmptyDirAndNotExistentWithoutPermissionDir() throws IOException {
        return new File[]{createNotRemovableDir(), new File("/home/bookie-impl-test")};
    }

    private static File[] getArrayWithExistentNotRemovableEmptyDirAndNotExistentWithPermissionDir() throws IOException {
        return new File[]{createNotRemovableDir(), new File("/tmp/bookie-impl-test")};
    }

    private static File[] getArrayWithAnExistentNotRemovableDirContainingDir() throws IOException {
        return new File[]{createNotRemovableDirContainingDir()};
    }

    private static File[] getArrayWithTwoExistentNotRemovableDirsContainingDir() throws IOException {
        return new File[]{createNotRemovableDirContainingDir(), createNotRemovableDirContainingDir()};
    }

    private static File[] getArrayWithAnExistentNotRemovableDirContainingDirAndExistentNotRemovableDirContainingFile() throws IOException {
        return new File[]{createNotRemovableDirContainingDir(), createNotRemovableDirContainingFile()};
    }

    private static File[] getArrayWithAnExistentNotRemovableDirContainingDirAndNotExistentWithoutPermissionDir() throws IOException {
        return new File[]{createNotRemovableDirContainingDir(), new File("/home/bookie-impl-test")};
    }

    private static File[] getArrayWithAnExistentNotRemovableDirContainingDirAndNotExistentWithPermissionDir() throws IOException {
        return new File[]{createNotRemovableDirContainingDir(), new File("/tmp/bookie-impl-test")};
    }

    private static File[] getArrayWithAnExistentNotRemovableDirContainingFile() throws IOException {
        return new File[]{createNotRemovableDirContainingFile()};
    }

    private static File[] getArrayWithTwoExistentNotRemovableDirsContainingFile() throws IOException {
        return new File[]{createNotRemovableDirContainingFile(),createNotRemovableDirContainingFile()};
    }

    private static File[] getArrayWithAnExistentNotRemovableDirContainingFileAndNotExistentWithoutPermissionDir() throws IOException {
       return new File[]{createNotRemovableDirContainingFile(), new File("/home/bookie-impl-test")};
    }

    private static File[] getArrayWithAnExistentNotRemovableDirContainingFileAndNotExistentWithPermissionDir() throws IOException {
        return new File[]{createNotRemovableDirContainingFile(), new File("/tmp/bookie-impl-test")};
    }

    private static File[] getArrayWithNotExistentWithoutPermissionDir() {
        return new File[]{new File("/home/bookie-impl-test")};
    }

    private static File[] getArrayWithTwoNotExistentWithoutPermissionDirs() {
        return new File[]{new File("/home/bookie-impl-test"),new File("/home/bookie-impl-test2")};
    }

    private static File[] getArrayWithNotExistentWithoutPermissionDirAndNotExistentWithPermissionDir() {
        return new File[]{new File("/home/bookie-impl-test"),new File("/tmp/bookie-impl-test")};
    }

    private static File[] getArrayWithNotExistentWithPermissionDir() {
        return new File[]{new File("/tmp/bookie-impl-test")};
    }

    private static File[] getArrayWithTwoNotExistentWithPermissionDirs() {
        return new File[]{new File("/tmp/bookie-impl-test"),new File("/tmp/bookie-impl-test2")};
    }

    public static File[] initConf(DirArrayEnum type) throws Exception {
        switch (type) {
            case EMPTY_ARRAY:
                return new File[]{};
            case ARRAY_WITH_NULL_DIR:
                return new File[]{null};
            case ARRAY_WITH_TWO_NULL_DIRS:
                return new File[]{null, null};
            case ARRAY_WITH_NULL_AND_FILE:
                return getArrayWithFileAndNull();
            case ARRAY_WITH_NULL_AND_EXISTENT_REMOVABLE_EMPTY_DIR:
                return getArrayWithNullAndExistentRemovableEmptyDir();
            case ARRAY_WITH_NULL_AND_EXISTENT_REMOVABLE_DIR_CONTAINING_DIR:
                return getArrayWithNullAndExistentRemovableDirContainingDir();
            case ARRAY_WITH_NULL_AND_EXISTENT_REMOVABLE_DIR_CONTAINING_FILE:
                return getArrayWithNullAndExistentRemovableDirContainingFile();
            case ARRAY_WITH_NULL_AND_EXISTENT_NOT_REMOVABLE_EMPTY_DIR:
                return getArrayWithNullAndExistentNotRemovableEmptyDir();
            case ARRAY_WITH_NULL_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_DIR:
                return getArrayWithNullAndExistentNotRemovableDirContainingDir();
            case ARRAY_WITH_NULL_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_FILE:
                return getArrayWithNullAndExistentNotRemovableDirContainingFile();
            case ARRAY_WITH_NULL_AND_NOT_EXISTENT_WITHOUT_PERMISSION_DIR:
                return getArrayWithNullAndNotExistentWithoutPermissionDir();
            case ARRAY_WITH_NULL_AND_NOT_EXISTENT_WITH_PERMISSION_DIR:
                return getArrayWithNullAndNotExistentWithPermissionDir();
            case ARRAY_WITH_FILE:
                return getArrayWithFile();
            case ARRAY_WITH_TWO_FILES:
                return getArrayWithFiles();
            case ARRAY_WITH_FILE_AND_EXISTENT_REMOVABLE_EMPTY_DIR:
                return getArrayWithFileAndExistentRemovableEmptyDir();
            case ARRAY_WITH_FILE_AND_EXISTENT_REMOVABLE_DIR_CONTAINING_DIR:
                return getArrayWithFileAndExistentRemovableDirContainingDir();
            case ARRAY_WITH_FILE_AND_EXISTENT_REMOVABLE_DIR_CONTAINING_FILE:
                return getArrayWithFileAndExistentRemovableDirContainingFile();
            case ARRAY_WITH_FILE_AND_EXISTENT_NOT_REMOVABLE_EMPTY_DIR:
                return getArrayWithFileAndExistentNotRemovableEmptyDir();
            case ARRAY_WITH_FILE_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_DIR:
                return getArrayWithFileAndExistentNotRemovableDirContainingDir();
            case ARRAY_WITH_FILE_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_FILE:
                return getArrayWithFileAndExistentNotRemovableDirContainingFile();
            case ARRAY_WITH_FILE_AND_NOT_EXISTENT_WITHOUT_PERMISSION_DIR:
                return getArrayWithFileAndNotExistentWithoutPermissionDir();
            case ARRAY_WITH_FILE_AND_NOT_EXISTENT_WITH_PERMISSION_DIR:
                return getArrayWithFileAndNotExistentWithPermissionDir();
            case ARRAY_WITH_EXISTENT_REMOVABLE_EMPTY_DIR:
                return getArrayWithExistentRemovableEmptyDir();
            case ARRAY_WITH_TWO_EXISTENT_REMOVABLE_EMPTY_DIRS:
                return getArrayWithTwoExistentRemovableEmptyDirs();
            case ARRAY_WITH_EXISTENT_REMOVABLE_EMPTY_DIR_AND_EXISTENT_REMOVABLE_DIR_CONTAINING_DIR:
                return getArrayWithExistentRemovableEmptyDirAndExistentRemovableDirContainingDir();
            case ARRAY_WITH_EXISTENT_REMOVABLE_EMPTY_DIR_AND_EXISTENT_REMOVABLE_DIR_CONTAINING_FILE:
                return getArrayWithExistentRemovableEmptyDirAndExistentRemovableDirContainingFile();
            case ARRAY_WITH_EXISTENT_REMOVABLE_EMPTY_DIR_AND_EXISTENT_NOT_REMOVABLE_EMPTY_DIR:
                return getArrayWithExistentRemovableEmptyDirAndExistentNotRemovableEmptyDir();
            case ARRAY_WITH_EXISTENT_REMOVABLE_EMPTY_DIR_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_DIR:
                return getArrayWithExistentRemovableEmptyDirAndExistentNotRemovableDirContainingDir();
            case ARRAY_WITH_EXISTENT_REMOVABLE_EMPTY_DIR_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_FILE:
                return getArrayWithExistentRemovableEmptyDirAndExistentNotRemovableDirContainingFile();
            case ARRAY_WITH_EXISTENT_REMOVABLE_EMPTY_DIR_AND_NOT_EXISTENT_WITHOUT_PERMISSION_DIR:
                return getArrayWithExistentRemovableEmptyDirAndNotExistentWithoutPermissionDir();
            case ARRAY_WITH_EXISTENT_REMOVABLE_EMPTY_DIR_AND_NOT_EXISTENT_WITH_PERMISSION_DIR:
                return getArrayWithExistentRemovableEmptyDirAndNotExistentWithPermissionDir();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_DIR:
                return getArrayWithAnExistentRemovableDirContainingDir();
            case ARRAY_WITH_TWO_EXISTENT_REMOVABLE_DIRS_CONTAINING_DIR:
                return getArrayWithTwoExistentRemovableDirsContainingDir();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_DIR_AND_EXISTENT_REMOVABLE_DIR_CONTAINING_FILE:
                return getArrayWithAnExistentRemovableDirContainingDirAndExistentRemovableDirContainingFile();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_DIR_AND_EXISTENT_NOT_REMOVABLE_EMPTY_DIR:
                return getArrayWithAnExistentRemovableDirContainingDirAndExistentNotRemovableEmptyDir();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_DIR_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_DIR:
                return getArrayWithAnExistentRemovableDirContainingDirAndExistentNotRemovableDirContainingDir();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_DIR_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_FILE:
                return getArrayWithAnExistentRemovableDirContainingDirAndExistentNotRemovableDirContainingFile();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_DIR_AND_NOT_EXISTENT_WITHOUT_PERMISSION_DIR:
                return getArrayWithAnExistentRemovableDirContainingDirAndNotExistentWithoutPermissionDir();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_DIR_AND_NOT_EXISTENT_WITH_PERMISSION_DIR:
                return getArrayWithAnExistentRemovableDirContainingDirAndNotExistentWithPermissionDir();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_FILE:
                return getArrayWithAnExistentRemovableDirContainingFile();
            case ARRAY_WITH_TWO_EXISTENT_REMOVABLE_DIRS_CONTAINING_FILE:
                return getArrayWithTwoExistentRemovableDirsContainingFile();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_FILE_AND_EXISTENT_NOT_REMOVABLE_EMPTY_DIR:
                return getArrayWithAnExistentRemovableDirContainingFileAndExistentNotRemovableEmptyDir();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_FILE_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_DIR:
                return getArrayWithAnExistentRemovableDirContainingFileAndExistentNotRemovableDirContainingDir();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_FILE_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_FILE:
                return getArrayWithAnExistentRemovableDirContainingFileAndExistentNotRemovableDirContainingFile();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_FILE_AND_NOT_EXISTENT_WITHOUT_PERMISSION_DIR:
                return getArrayWithAnExistentRemovableDirContainingFileAndNotExistentWithoutPermissionDir();
            case ARRAY_WITH_AN_EXISTENT_REMOVABLE_DIR_CONTAINING_FILE_AND_NOT_EXISTENT_WITH_PERMISSION_DIR:
                return getArrayWithAnExistentRemovableDirContainingFileAndNotExistentWithPermissionDir();
            case ARRAY_WITH_EXISTENT_NOT_REMOVABLE_EMPTY_DIR:
                return DirsArrayBuilder.getArrayWithExistentNotRemovableEmptyDir();
            case ARRAY_WITH_TWO_EXISTENT_NOT_REMOVABLE_EMPTY_DIRS:
                return DirsArrayBuilder.getArrayWithTwoExistentNotRemovableEmptyDirs();
            case ARRAY_WITH_EXISTENT_NOT_REMOVABLE_EMPTY_DIR_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_DIR:
                return DirsArrayBuilder.getArrayWithExistentNotRemovableEmptyDirAndExistentNotRemovableDirContainingDir();
            case ARRAY_WITH_EXISTENT_NOT_REMOVABLE_EMPTY_DIR_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_FILE:
                return DirsArrayBuilder.getArrayWithExistentNotRemovableEmptyDirAndExistentNotRemovableDirContainingFile();
            case ARRAY_WITH_EXISTENT_NOT_REMOVABLE_EMPTY_DIR_AND_NOT_EXISTENT_WITHOUT_PERMISSION_DIR:
                return DirsArrayBuilder.getArrayWithExistentNotRemovableEmptyDirAndNotExistentWithoutPermissionDir();
            case ARRAY_WITH_EXISTENT_NOT_REMOVABLE_EMPTY_DIR_AND_NOT_EXISTENT_WITH_PERMISSION_DIR:
                return DirsArrayBuilder.getArrayWithExistentNotRemovableEmptyDirAndNotExistentWithPermissionDir();
            case ARRAY_WITH_AN_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_DIR:
                return DirsArrayBuilder.getArrayWithAnExistentNotRemovableDirContainingDir();
            case ARRAY_WITH_TWO_EXISTENT_NOT_REMOVABLE_DIRS_CONTAINING_DIR:
                return DirsArrayBuilder.getArrayWithTwoExistentNotRemovableDirsContainingDir();
            case ARRAY_WITH_AN_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_DIR_AND_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_FILE:
                return DirsArrayBuilder.getArrayWithAnExistentNotRemovableDirContainingDirAndExistentNotRemovableDirContainingFile();
            case ARRAY_WITH_AN_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_DIR_AND_NOT_EXISTENT_WITHOUT_PERMISSION_DIR:
                return DirsArrayBuilder.getArrayWithAnExistentNotRemovableDirContainingDirAndNotExistentWithoutPermissionDir();
            case ARRAY_WITH_AN_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_DIR_AND_NOT_EXISTENT_WITH_PERMISSION_DIR:
                return DirsArrayBuilder.getArrayWithAnExistentNotRemovableDirContainingDirAndNotExistentWithPermissionDir();
            case ARRAY_WITH_AN_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_FILE:
                return DirsArrayBuilder.getArrayWithAnExistentNotRemovableDirContainingFile();
            case ARRAY_WITH_TWO_EXISTENT_NOT_REMOVABLE_DIRS_CONTAINING_FILE:
                return DirsArrayBuilder.getArrayWithTwoExistentNotRemovableDirsContainingFile();
            case ARRAY_WITH_AN_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_FILE_AND_NOT_EXISTENT_WITHOUT_PERMISSION_DIR:
                return DirsArrayBuilder.getArrayWithAnExistentNotRemovableDirContainingFileAndNotExistentWithoutPermissionDir();
            case ARRAY_WITH_AN_EXISTENT_NOT_REMOVABLE_DIR_CONTAINING_FILE_AND_NOT_EXISTENT_WITH_PERMISSION_DIR:
                return DirsArrayBuilder.getArrayWithAnExistentNotRemovableDirContainingFileAndNotExistentWithPermissionDir();
            case ARRAY_WITH_NOT_EXISTENT_WITHOUT_PERMISSION_DIR:
                return DirsArrayBuilder.getArrayWithNotExistentWithoutPermissionDir();
            case ARRAY_WITH_TWO_NOT_EXISTENT_WITHOUT_PERMISSION_DIRS:
                return DirsArrayBuilder.getArrayWithTwoNotExistentWithoutPermissionDirs();
            case ARRAY_WITH_NOT_EXISTENT_WITHOUT_PERMISSION_AND_NOT_EXISTENT_WITH_PERMISSION_DIR:
                return DirsArrayBuilder.getArrayWithNotExistentWithoutPermissionDirAndNotExistentWithPermissionDir();
            case ARRAY_WITH_NOT_EXISTENT_WITH_PERMISSION_DIR:
                return DirsArrayBuilder.getArrayWithNotExistentWithPermissionDir();
            case ARRAY_WITH_TWO_NOT_EXISTENT_WITH_PERMISSION_DIRS:
                return DirsArrayBuilder.getArrayWithTwoNotExistentWithPermissionDirs();
            default:
                return null;
        }
    }
}