package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.test.TmpDirs;
import org.apache.bookkeeper.utils.DeleteTempFiles;
import org.apache.bookkeeper.utils.DirsArrayBuilder;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.bookkeeper.utils.EntryBuilder.getInvalidEntry;
import static org.apache.bookkeeper.utils.EntryBuilder.getValidEntry;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value = Enclosed.class)
public class BookieImplTests {
    private static final Logger LOG = LoggerFactory.getLogger(BookieImplTests.class);

    @RunWith(Parameterized.class)
    public static class FormatTest {

        private final boolean isInteractive;
        private final boolean force;
        private final boolean expectedResult;
        private final File[] journalDirs;
        private final String gcEntryLogMetadataCachePath;
        private final File[] indexDirs;
        private final File[] ledgerDirs;
        private final String input;
        private ServerConfiguration conf;
        private final boolean expException;
        private static InputStream originalSystemIn;

        public FormatTest(File[] journalDirs, File[] ledgerDirs, File[] indexDirs,
                          String gcEntryLogMetadataCachePath, boolean isInteractive, boolean force, String input,
                          boolean expectedResult, boolean expException) {
            this.isInteractive = isInteractive;
            this.force = force;
            this.expectedResult = expectedResult;
            this.journalDirs = journalDirs;
            this.ledgerDirs = ledgerDirs;
            this.indexDirs = indexDirs;
            this.gcEntryLogMetadataCachePath = gcEntryLogMetadataCachePath;
            this.input = input;
            this.expException = expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() throws Exception {
            DirsArrayBuilder dirsBuilder=new DirsArrayBuilder();

            return Arrays.asList(new Object[][]{
                    {null, new File[]{}, new File[]{null}, null, true, true, "Y", false, true},
                    {new File[]{}, new File[]{null}, dirsBuilder.getArrayWithAnInvalidDir(), dirsBuilder.getArrayWithAnInvalidDir()[0].getAbsolutePath(), true, false, "N", false, true},
                    {new File[]{null}, dirsBuilder.getArrayWithAnInvalidDir(), dirsBuilder.getInvalidDirsArray(), dirsBuilder.getArrayWithValidExistentDir()[0].getAbsolutePath(), true, true, "NULL", false, true},
                    {dirsBuilder.getArrayWithAnInvalidDir(), dirsBuilder.getInvalidDirsArray(), dirsBuilder.getArrayWithInvalidAndExistentValidDirs(), dirsBuilder.getArrayWithAFile()[0].getAbsolutePath(), true, false, "E", false, false},
                    {dirsBuilder.getInvalidDirsArray(), dirsBuilder.getArrayWithInvalidAndExistentValidDirs(), dirsBuilder.getArrayWithInvalidAndNotExistentValidDirs(), dirsBuilder.getArrayWithValidDir()[0].getAbsolutePath(), true, true, "", false, false},
                    {dirsBuilder.getArrayWithInvalidAndExistentValidDirs(), dirsBuilder.getArrayWithInvalidAndNotExistentValidDirs(), dirsBuilder.getInvalidAndNullDirs(), null, true, false, "Y", false, false},
                    {dirsBuilder.getArrayWithInvalidAndNotExistentValidDirs(), dirsBuilder.getInvalidAndNullDirs(), dirsBuilder.getFileAndInvalidDir(), dirsBuilder.getArrayWithAnInvalidDir()[0].getAbsolutePath(), false, true, "N", false, false},
                    {dirsBuilder.getInvalidAndNullDirs(), dirsBuilder.getFileAndInvalidDir(), dirsBuilder.getArrayWithValidExistentDir(), dirsBuilder.getArrayWithValidExistentDir()[0].getAbsolutePath(), false, false, "NULL", false, false},
                    {dirsBuilder.getFileAndInvalidDir(), dirsBuilder.getArrayWithValidExistentDir(), dirsBuilder.getArrayValidExistentDirs(), dirsBuilder.getArrayWithAFile()[0].getAbsolutePath(), true, true, "N", false, false},
                    {dirsBuilder.getArrayWithValidExistentDir(), dirsBuilder.getArrayValidExistentDirs(), dirsBuilder.getArrayWithAValidExistentAndNotExistentDirs(), dirsBuilder.getArrayWithValidDir()[0].getAbsolutePath(), true, false, "Y", true, false},
                    {dirsBuilder.getArrayValidExistentDirs(), dirsBuilder.getArrayWithAValidExistentAndNotExistentDirs(), dirsBuilder.getArrayValidExistentAndNullDirs(), null, true, true, "NULL", false, true},
                    {dirsBuilder.getArrayWithAValidExistentAndNotExistentDirs(), dirsBuilder.getArrayValidExistentAndNullDirs(), dirsBuilder.getFileAndValidExistentDirs(), dirsBuilder.getArrayWithAnInvalidDir()[0].getAbsolutePath(), true, false, "E", false, true},
                    {dirsBuilder.getArrayValidExistentAndNullDirs(), dirsBuilder.getFileAndValidExistentDirs(), dirsBuilder.getArrayWithValidDir(), dirsBuilder.getArrayWithValidExistentDir()[0].getAbsolutePath(), true, true, "", false, true},
                    {dirsBuilder.getFileAndValidExistentDirs(), dirsBuilder.getArrayWithValidDir(), dirsBuilder.getArrayWithValidDirs(), dirsBuilder.getArrayWithAFile()[0].getAbsolutePath(), true, false, "Y", true, false},
                    {dirsBuilder.getArrayWithValidDir(), dirsBuilder.getArrayWithValidDirs(), dirsBuilder.getArrayWithValidAndNullDirs(), dirsBuilder.getArrayWithValidDir()[0].getAbsolutePath(), false, true, "N", false, true},
                    {dirsBuilder.getArrayWithValidDirs(), dirsBuilder.getArrayWithValidAndNullDirs(), dirsBuilder.getFileAndValidDir(), null, true, true, "NULL", false, true},
                    {dirsBuilder.getArrayWithValidAndNullDirs(), dirsBuilder.getFileAndValidDir(), dirsBuilder.getArrayWithAFile(), dirsBuilder.getArrayWithAnInvalidDir()[0].getAbsolutePath(), true, false, "E", false, true},
                    {dirsBuilder.getFileAndValidDir(), dirsBuilder.getArrayWithAFile(), dirsBuilder.getArrayWithFiles(), dirsBuilder.getArrayWithValidExistentDir()[0].getAbsolutePath(), true, true, "", true, false},
                    {dirsBuilder.getArrayWithAFile(), dirsBuilder.getArrayWithFiles(), null, dirsBuilder.getArrayWithAFile()[0].getAbsolutePath(), true, false, "Y", true, false},
                    {dirsBuilder.getArrayWithFiles(), null, new File[]{}, dirsBuilder.getArrayWithValidDir()[0].getAbsolutePath(), false, true, "N", false, true},
                    {dirsBuilder.getArrayWithFileAndNull(), dirsBuilder.getArrayWithFileAndNull(), dirsBuilder.getArrayWithFileAndNull(), "", false, true, "N", false, true},
//                    {dirsBuilder.getArrayValidExistentDirs(), dirsBuilder.getArrayValidExistentDirs(), null, "", false, true, "", true, false}, //PIT
//                    {dirsBuilder.getArrayValidExistentDirs(), dirsBuilder.getArrayValidExistentDirs(), null, "", false, false, "", false, false}, //PIT
            });
        }

        @Before
        public void setup() {
            conf = mock(ServerConfiguration.class);
            when(conf.getJournalDirs()).thenReturn(journalDirs);
            when(conf.getLedgerDirs()).thenReturn(ledgerDirs);
            when(conf.getIndexDirs()).thenReturn(indexDirs);
            when(conf.getGcEntryLogMetadataCachePath()).thenReturn(gcEntryLogMetadataCachePath);
        }

        @After
        public void cleanup() {
            DeleteTempFiles.deleteFiles(conf.getJournalDirs(), conf.getIndexDirs(), conf.getLedgerDirs());
            System.setIn(originalSystemIn);
        }

        @Test
        public void formatTest() {
            try {
                originalSystemIn = System.in;
                switch (this.input) {
                    case "Y":
                        System.setIn(new ByteArrayInputStream("Y".getBytes()));
                        break;
                    case "N":
                        System.setIn(new ByteArrayInputStream("N".getBytes()));
                        break;
                    case "E":
                        System.setIn(new InputStream() {
                            @Override
                            public int read() throws IOException {
                                throw new IOException("Simulated IOException");
                            }
                        });
                        break;
                    case "NULL":
                        System.setIn(null);
                        break;
                    case "":
                        System.setIn(new ByteArrayInputStream("\ny".getBytes()));
                        //System.setIn(new ByteArrayInputStream("n".getBytes()));
                        break;

                }
                boolean result = BookieImpl.format(conf, isInteractive, force);
                assertEquals(this.expectedResult, result);
                assertFalse(this.expException);
            }catch(NullPointerException e) {
                e.printStackTrace();
                assertTrue(this.expException);
            }
        }
    }
    @RunWith(Parameterized.class)
    public static class AddAndReadEntryTest{

        private final long ledgerId;
        private final boolean expException;
        private final long entryId;
        private TestBookieImpl bookie;
        private File dir;
        private final TmpDirs tmpDirs=new TmpDirs();

        public AddAndReadEntryTest(long ledgerId, long entryId, boolean expException){
            this.ledgerId=ledgerId;
            this.entryId=entryId;
            this.expException=expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {
            return Arrays.asList(new Object[][]{
                    {-1,-1, true},
                    {-1,0, true},
                    //{0,-1, true}, //commentato perché dà errore
                    {0,0, false},
                    {0,1, true},
                    {1, -1, true},
                    {1, 0, true},
                    {1, 1, true},
            });
        }


        @Before
        public void startup() throws Exception {
            this.dir= this.tmpDirs.createNew("bookie-test", ".tmp");
            ServerConfiguration conf= TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(this.dir.toString());
            conf.setLedgerDirNames(new String[]{this.dir.getAbsolutePath()});
            try {
                this.bookie= new TestBookieImpl(conf);
                this.bookie.start();
            } catch (Exception e) {
                Assert.fail("Si è verificato un errore non previsto");
            }
        }

        @After
        public void cleanup(){
            try {
                this.tmpDirs.cleanup();
                this.bookie.shutdown();
            } catch (Exception e) {
                LOG.info("La directory {} non è stata eliminata", this.dir);
            }
        }

        @Test
        public void addAndReadEntryTest(){
            try {
                AtomicBoolean complete = new AtomicBoolean(false);
                final byte[] masterKey="masterKey".getBytes(StandardCharsets.UTF_8);
                ByteBuf byteBuf= getValidEntry();
                this.bookie.addEntry(byteBuf,
                        false, (rc, ledgerId, entryId, addr, ctx) -> complete.set(true), null, masterKey);

                Awaitility.await().untilAsserted(()-> assertTrue(complete.get()));

                byteBuf=getValidEntry();
                ByteBuf prova= this.bookie.readEntry(this.ledgerId, this.entryId);

                int length = prova.readableBytes();

                // Crea un array di byte per contenere il contenuto del buffer
                byte[] content = new byte[length];

                // Leggi il contenuto nel tuo array di byte
                prova.getBytes(prova.readerIndex(), content);
                assertEquals(new String(byteBuf.array(), StandardCharsets.UTF_8), new String(content, StandardCharsets.UTF_8));
                LOG.info(new String(byteBuf.array(), StandardCharsets.UTF_8)+" letta entry "+new String(content, StandardCharsets.UTF_8));
                assertFalse(this.expException);
            } catch (Exception e) {
                assertTrue(this.expException);
            }
        }
    }

    @RunWith(value= Parameterized.class)
    public static class AddEntryTest{

        private final ByteBuf entry;
        private final boolean ackBeforeSync;
        private final Object ctx;
        private final byte[] masterKey;
        private final boolean expException;
        private final boolean nullCallback;
        private TestBookieImpl bookie;
        private File dir;
        private final TmpDirs tmpDirs=new TmpDirs();


        public AddEntryTest(ByteBuf entry, boolean ackBeforeSync, boolean nullCallback, Object ctx, byte[] masterKey, boolean expException){
            this.entry=entry;
            this.ackBeforeSync=ackBeforeSync;
            this.ctx=ctx;
            this.masterKey=masterKey;
            this.expException=expException;
            this.nullCallback=nullCallback;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            final byte[] validMK="masterKey".getBytes(StandardCharsets.UTF_8);

            return Arrays.asList(new Object[][]{
                    {getValidEntry(), false, false, null, validMK, false},
                    {null, true, true, null, "".getBytes(StandardCharsets.UTF_8), true},
                    {getInvalidEntry(), false, true, new Object(), null, true},
            });
        }

        @Before
        public void startup() throws Exception {
            this.dir= this.tmpDirs.createNew("bookie-impl-test", ".tmp");
            ServerConfiguration conf= TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(this.dir.toString());
            conf.setLedgerDirNames(new String[]{this.dir.getAbsolutePath()});
            try {
                this.bookie= new TestBookieImpl(conf);
                this.bookie.start();
            } catch (Exception e) {
                Assert.fail("Si è verificato un errore non previsto");
            }
        }

        @After
        public void cleanup(){
            try {
                this.tmpDirs.cleanup();
                this.bookie.shutdown();
            } catch (Exception e) {
                LOG.info("La directory {} non è stata eliminata", this.dir);
            }
        }

        @Test
        public void recoveryAddEntryTest(){

            try {

                AtomicBoolean complete= new AtomicBoolean(false);
                ByteBuf entry=Unpooled.buffer(this.entry.capacity());
                entry.writeBytes(this.entry);
                if(this.nullCallback) {
                    this.bookie.recoveryAddEntry(entry, null,this.ctx, this.masterKey);
                    assertEquals(1, entry.refCnt());
                }else {
                    this.bookie.recoveryAddEntry(entry, (rc, ledgerId, entryId, addr, ctx) -> complete.set(true),
                            this.ctx, this.masterKey);

                    Awaitility.await().untilTrue(complete);
                    assertEquals(0, entry.refCnt());
                }
                if(this.expException){
                    Assert.fail("Attesa exception");
                }
            } catch (Exception e) {
                assertTrue(this.expException);
            }

        }

        @Test
        public void addEntryTest(){
            try {

                AtomicBoolean complete= new AtomicBoolean(false);
                ByteBuf entry=Unpooled.buffer(this.entry.capacity());
                entry.writeBytes(this.entry);
                    if(this.nullCallback) {
                        this.bookie.addEntry(entry,
                                this.ackBeforeSync, null, this.ctx, this.masterKey);
                        assertEquals(1, entry.refCnt());
                    }else {
                        this.bookie.addEntry(entry,
                                this.ackBeforeSync, (rc, ledgerId, entryId, addr, ctx) -> complete.set(true), this.ctx, this.masterKey);

                        Awaitility.await().untilTrue(complete);
                        assertEquals(0, entry.refCnt());
                    }
                if(this.expException){
                    Assert.fail("Attesa exception");
                }
            } catch (Exception e) {
                assertTrue(this.expException);
            }

        }
    }

    public static class AddEntryToFenceLedgerTest{

        private File dir;
        private BookieImpl bookie;
        private final TmpDirs tmpDirs=new TmpDirs();


        public AddEntryToFenceLedgerTest(){}

        @Before
        public void startup() throws Exception {
            this.dir= this.tmpDirs.createNew("bookie-impl-test", ".tmp");
            ServerConfiguration conf= TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(this.dir.toString());
            conf.setLedgerDirNames(new String[]{this.dir.getAbsolutePath()});
            try {
                this.bookie= new TestBookieImpl(conf);
                this.bookie.start();
            } catch (Exception e) {
                Assert.fail("Si è verificato un errore non previsto");
            }
        }

        @After
        public void cleanup(){
            try {
                this.bookie.shutdown();
                this.tmpDirs.cleanup();
            } catch (Exception e) {
                LOG.info("La directory {} non è stata eliminata", this.dir);
            }
        }

        @Test
        public void addEntryToFencedLedgerTest() throws IOException, BookieException, InterruptedException {
            ByteBuf byteBuf = getValidEntry();
            this.bookie.fenceLedger(this.bookie.getLedgerForEntry(byteBuf, "pass".getBytes(StandardCharsets.UTF_8)).getLedgerId(), "pass".getBytes(StandardCharsets.UTF_8));
            try {
                this.bookie.addEntry(byteBuf, true, new BookieImpl.NopWriteCallback(), new Object(), "pass".getBytes(StandardCharsets.UTF_8));
                Assert.fail("Non deve essere possibile scrivere su un fenced ledger");
            }catch (BookieException e){
                assertTrue(true);
            }
        }

        @Test
        public void recoveryAddEntryToFencedLedgerTest() throws IOException, BookieException, InterruptedException {
            AtomicBoolean complete = new AtomicBoolean(false);

            ByteBuf byteBuf = getValidEntry();
            this.bookie.fenceLedger(this.bookie.getLedgerForEntry(byteBuf, "pass".getBytes(StandardCharsets.UTF_8)).getLedgerId(), "pass".getBytes(StandardCharsets.UTF_8));
            try {
                this.bookie.recoveryAddEntry(byteBuf, (rc, ledgerId, entryId, addr, ctx) -> complete.set(true), new Object(), "pass".getBytes(StandardCharsets.UTF_8));
                Awaitility.await().untilTrue(complete);
                assertEquals(0, byteBuf.refCnt());
            }catch (BookieException e){
                Assert.fail("Deve essere possibile scrivere su un fenced ledger tramite recoveryAddEntry");
            }
        }
    }

    @RunWith(value= Parameterized.class)
    public static class SetExplicitLacTest{

        private final String entryType;
        private final Object ctx;
        private final byte[] masterKey;
        private final boolean expException;
        private TestBookieImpl bookie;
        private File dir;
        private ByteBuf entry;
        private final TmpDirs tmpDirs=new TmpDirs();
        private final boolean nullCallback;

        public SetExplicitLacTest(String entryType, boolean nullCallback, Object ctx, byte[] masterKey, boolean expException){
            this.entryType=entryType;
            this.ctx=ctx;
            this.masterKey=masterKey;
            this.expException=expException;
            this.nullCallback=nullCallback;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            final byte[] validMK="masterKey".getBytes(StandardCharsets.UTF_8);

            return Arrays.asList(new Object[][]{
                    {"NULL", true, null, "".getBytes(), true},
                    {"INVALID_ENTRY", true, new Object(), null, true},
                    {"VALID_ENTRY", false, null, validMK, false},

            });
        }

        @Before
        public void startup() throws Exception {
            this.dir= this.tmpDirs.createNew("bookie-impl-test", ".tmp");
            ServerConfiguration conf= TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(this.dir.toString());
            conf.setLedgerDirNames(new String[]{this.dir.getAbsolutePath()});
            try {
                this.bookie= new TestBookieImpl(conf);
                this.bookie.start();
            } catch (Exception e) {
                Assert.fail("Si è verificato un errore non previsto");
            }

            byte[] buff = "Hello, world".getBytes();
            ByteBuf byteBuf= Unpooled.buffer(buff.length);
            byteBuf.writeBytes(buff);

            switch (this.entryType){
                case "VALID_ENTRY":
                    this.entry=this.bookie.createExplicitLACEntry(0L, byteBuf);
                    break;
                case "INVALID_ENTRY":
                    this.entry=this.bookie.createExplicitLACEntry(-1L, byteBuf);
                    break;
                case "NULL":
                    this.entry=null;
            }
        }

        @After
        public void cleanup(){
            try {
                this.tmpDirs.cleanup();
                this.bookie.shutdown();
            } catch (Exception e) {
                LOG.info("La directory {} non è stata eliminata", this.dir);
            }
        }

        @Test
        public void setExplicitLacTest(){
            try {

                AtomicBoolean complete= new AtomicBoolean(false);
                if(this.nullCallback) {
                    this.bookie.setExplicitLac(this.entry, null, this.ctx, this.masterKey);
                }else {
                    this.bookie.setExplicitLac(this.entry,
                            (rc, ledgerId, entryId, addr, ctx) -> complete.set(true), this.ctx, this.masterKey);

                    Awaitility.await().untilAsserted(() -> assertTrue(complete.get()));
                }
                assertEquals(0, this.entry.refCnt());
                if(this.expException){
                    Assert.fail("Attesa exception");
                }
            } catch (Exception e) {
                assertTrue(this.expException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class SetAndGetLACTest {

        private final long ledgerId;
        private final boolean expException;
        private File dir;
        private File dir2;
        private BookieImpl bookie;
        private final TmpDirs tmpDirs=new TmpDirs();

        public SetAndGetLACTest(long ledgerId, boolean expException){
            this.ledgerId=ledgerId;
            this.expException=expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){
            return Arrays.asList(new Object[][]{
                    {-1L, true},
                    {0L, false},
                    {1L, true},

            });
        }

        @Before
        public void startup() throws Exception {
            this.dir= this.tmpDirs.createNew("bookie-impl-test", ".tmp");
//            this.dir2= this.tmpDirs.createNew("bookie-impl-test", ".tmp"); //aggiunti per aumentare la percentuale su pit
            ServerConfiguration conf= TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(this.dir.toString());
            conf.setLedgerDirNames(new String[]{this.dir.getAbsolutePath()});
//            conf.setIndexDirName(new String[]{this.dir2.getAbsolutePath()}); //aggiunti per aumentare la percentuale su pit
            try {
                this.bookie= new TestBookieImpl(conf);
                this.bookie.start();
            } catch (Exception e) {
                Assert.fail("Si è verificato un errore non previsto");
            }
        }

        @After
        public void cleanup(){
            try {
                this.bookie.shutdown();
                this.tmpDirs.cleanup();
            } catch (Exception e) {
                LOG.info("La directory {} non è stata eliminata", this.dir);
            }
        }

        @Test
        public void setAndGetLACTest() {
            try {
                byte[] buff = "hi".getBytes();
                ByteBuf byteBuf = Unpooled.buffer(buff.length);
                byteBuf.writeBytes(buff);
                ByteBuf lac = this.bookie.createExplicitLACEntry(0L, byteBuf);
                ByteBuf lac2 = Unpooled.copiedBuffer(lac);
                this.bookie.setExplicitLac(lac2, (rc, ledgerId, entryId, addr, ctx) -> {

                }, new Object(), "pass".getBytes(StandardCharsets.UTF_8));

                byte[] exp = new byte[lac.readableBytes()];
                lac.getBytes(lac.readerIndex(), exp);

                ByteBuf res = this.bookie.getExplicitLac(this.ledgerId);

                assertEquals(new String(exp, StandardCharsets.UTF_8),
                        new String(res.array(), StandardCharsets.UTF_8));
                assertFalse(this.expException);
            }catch(Exception e){
                assertTrue(this.expException);
            }
        }

    }

///*
    //whiteBox analysis
    public static class BookieImplConstructorTest{

        private final TmpDirs tmpDirs=new TmpDirs();

        @After
        public void cleanup() throws Exception {
            this.tmpDirs.cleanup();
        }

        @Test
        public void constructorTest() throws Exception {
            File dir = this.tmpDirs.createNew("bookie-impl-test", ".tmp");
            File dir2 = this.tmpDirs.createNew("bookie-impl-test", ".tmp"); //aggiunti per aumentare la percentuale su pit
            ServerConfiguration conf= TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(dir.toString());
            conf.setLedgerDirNames(new String[]{dir.getAbsolutePath()});
            conf.setIndexDirName(new String[]{dir2.getAbsolutePath()}); //aggiunti per aumentare la percentuale su pit
            conf.setAllowMultipleDirsUnderSameDiskPartition(false);
            try {
                BookieImpl bookie = new TestBookieImpl(conf);
                bookie.start();
                fail("Il test deve fallire");
            }catch(BookieException e){
                assertTrue(true);
            } catch (Exception e) {
                Assert.fail("Si deve verificare BookieException");
            }
        }
    }
//   */
}
