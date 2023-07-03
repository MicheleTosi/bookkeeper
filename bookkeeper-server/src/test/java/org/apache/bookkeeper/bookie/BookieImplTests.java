package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.test.TmpDirs;
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

import static org.apache.bookkeeper.utils.DirsArrayBuilder.*;
import static org.apache.bookkeeper.utils.EntryBuilder.getInvalidEntry;
import static org.apache.bookkeeper.utils.EntryBuilder.getValidEntry;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(value = Enclosed.class)
public class BookieImplTests {
    private static final Logger LOG = LoggerFactory.getLogger(BookieImplTests.class);

    @RunWith(Parameterized.class)
    public static class FormatTest{

        private final boolean isInteractive;
        private final boolean force;
        private final boolean expectedResult;
        private final File[] journalDirs;
        private final String gcEntryLogMetadataCachePath;
        private final File[] indexDirs;
        private final File[] ledgerDirs;
        private final String input;
        private ServerConfiguration conf;

        public FormatTest(File[] journalDirs,File[] ledgerDirs,File[] indexDirs,
                String gcEntryLogMetadataCachePath, boolean isInteractive, boolean force, String input,
                                                        boolean expectedResult) {
            this.isInteractive = isInteractive;
            this.force=force;
            this.expectedResult=expectedResult;
            this.journalDirs=journalDirs;
            this.ledgerDirs=ledgerDirs;
            this.indexDirs=indexDirs;
            this.gcEntryLogMetadataCachePath=gcEntryLogMetadataCachePath;
            this.input=input;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() throws Exception {
            return Arrays.asList(new Object[][]{
                    //{null, new File[]{}, new File[]{null}, null, true, true, "Y", false},
                    {new File[]{}, new File[]{null}, getArrayWithAnInvalidDir(), getArrayWithAnInvalidDir()[0].getAbsolutePath(), true, false, "N", false},
                    {new File[]{null}, getArrayWithAnInvalidDir(), getInvalidDirsArray(), getArrayWithValidExistentDir()[0].getAbsolutePath(), true, true, "NULL", false},
                    {getArrayWithAnInvalidDir(), getInvalidDirsArray(), getArrayWithInvalidAndExistentValidDirs(), getArrayWithAFile()[0].getAbsolutePath(), true, false, "E", false},
                    {getInvalidDirsArray(), getArrayWithInvalidAndExistentValidDirs(), getArrayWithInvalidAndNotExistentValidDirs(), getArrayWithValidDir()[0].getAbsolutePath(), true, true, "", false},
                    {getArrayWithInvalidAndExistentValidDirs(), getArrayWithInvalidAndNotExistentValidDirs(), getInvalidAndNullDirs(), null, true, false, "Y", false},
                    {getArrayWithInvalidAndNotExistentValidDirs(), getInvalidAndNullDirs(), getFileAndInvalidDir(), getArrayWithAnInvalidDir()[0].getAbsolutePath(), false, true, "N", false},
                    {getInvalidAndNullDirs(), getFileAndInvalidDir(), getArrayWithValidExistentDir(), getArrayWithValidExistentDir()[0].getAbsolutePath(), false, false, "NULL", false},
                    {getFileAndInvalidDir(), getArrayWithValidExistentDir(), getArrayValidExistentDirs(), getArrayWithAFile()[0].getAbsolutePath(), true, true, "N", false},
                    {getArrayWithValidExistentDir(), getArrayValidExistentDirs(), getArrayValidDirs(), getArrayWithValidDir()[0].getAbsolutePath(), true, false, "T", true},
                    {getArrayValidExistentDirs(), getArrayValidDirs(), getArrayValidExistentAndNullDirs(), null, true, true, "NULL", false},
                    {getArrayValidDirs(), getArrayValidExistentAndNullDirs(), getFileAndValidExistentDirs(), getArrayWithAnInvalidDir()[0].getAbsolutePath(), true, false, "E", false},
                    {getArrayValidExistentAndNullDirs(), getFileAndValidExistentDirs(), getArrayWithValidDir(), getArrayWithValidExistentDir()[0].getAbsolutePath(), true, true, "", true},
                    {getFileAndValidExistentDirs(), getArrayWithValidDir(), getArrayWithValidDirs(), getArrayWithAFile()[0].getAbsolutePath(), true, false, "Y", true},
                    {getArrayWithValidDir(), getArrayWithValidDirs(), getArrayWithValidAndNullDirs(), getArrayWithValidDir()[0].getAbsolutePath(), false, true, "N", true},
                    {getArrayWithValidDirs(), getArrayWithValidAndNullDirs(), getFileAndValidDir(), null, true, true, "NULL", false},
                    {getArrayWithValidAndNullDirs(), getFileAndValidDir(), getArrayWithAFile(), getArrayWithAnInvalidDir()[0].getAbsolutePath(), true, false, "E", false},
                    {getFileAndValidDir(), getArrayWithAFile(), getArrayWithFiles(), getArrayWithValidExistentDir()[0].getAbsolutePath(), true, true, "", true},
                    {getArrayWithAFile(), getArrayWithFiles(), null, getArrayWithAFile()[0].getAbsolutePath(), true, false, "Y", true},
                    {getArrayWithFiles(), null, new File[]{}, getArrayWithValidDir()[0].getAbsolutePath(), false, true, "N", false},

                    //pit
                    {getArrayWithAFile(), getArrayWithFiles(), null, getArrayWithAFile()[0].getAbsolutePath(), false, false, "Y", false},
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
        public void cleanup() throws Exception {
            DirsArrayBuilder.cleanup();
        }

        @Test
        public void formatTest() {
            Thread testThread=new Thread(()->{
                try {
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
                            break;
                    }
                    boolean result = BookieImpl.format(conf, isInteractive, force);
                    assertEquals(result, this.expectedResult);
                }catch(NullPointerException e) {
                    e.printStackTrace();
                    assertSame("NULL", this.input);
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
            testThread.start();
            try {
                testThread.join(20000);
            } catch (InterruptedException e) {
                if("".equals(this.input)) {
                    assertEquals("", this.input);
                }else{
                    fail("Non dovrebbe esserci un timeout");
                }
            }
        }

    }

    public static class AddAndReadEntryTest{

        private TestBookieImpl bookie;
        private File dir;
        private final TmpDirs tmpDirs=new TmpDirs();

        public AddAndReadEntryTest(){

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
                e.printStackTrace();
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
                ByteBuf prova= this.bookie.readEntry(this.bookie.getLedgerForEntry(byteBuf, masterKey).getLedgerId(),
                        byteBuf.getLong(byteBuf.readerIndex() + 8));

                int length = prova.readableBytes();

                // Crea un array di byte per contenere il contenuto del buffer
                byte[] content = new byte[length];

                // Leggi il contenuto nel tuo array di byte
                prova.getBytes(prova.readerIndex(), content);
                assertEquals(new String(byteBuf.array(), StandardCharsets.UTF_8), new String(content, StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException(e);
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
        private TestBookieImpl bookie;
        private File dir;
        private final TmpDirs tmpDirs=new TmpDirs();


        public AddEntryTest(ByteBuf entry, boolean ackBeforeSync, Object ctx, byte[] masterKey, boolean expException){
            this.entry=entry;
            this.ackBeforeSync=ackBeforeSync;
            this.ctx=ctx;
            this.masterKey=masterKey;
            this.expException=expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            final byte[] validMK="masterKey".getBytes(StandardCharsets.UTF_8);

            return Arrays.asList(new Object[][]{
                    {getValidEntry(), false, null, validMK, false},
                    {null, true, null, validMK, true},
                    {getInvalidEntry(), true, new Object(), null, true},
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
                e.printStackTrace();
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


                this.bookie.recoveryAddEntry(entry,(rc, ledgerId, entryId, addr, ctx) -> complete.set(true),
                        this.ctx, this.masterKey);

                Awaitility.await().untilTrue(complete);
                assertEquals(0, entry.refCnt());
                if(this.expException){
                    Assert.fail("Attesa exception");
                }
            } catch (Exception e) {
                e.printStackTrace();
                assertTrue(this.expException);
            }
        }

        @Test
        public void addEntryTest(){
            try {

                AtomicBoolean complete= new AtomicBoolean(false);
                ByteBuf entry=Unpooled.buffer(this.entry.capacity());
                entry.writeBytes(this.entry);

                this.bookie.addEntry(entry,
                        this.ackBeforeSync, (rc, ledgerId, entryId, addr, ctx) -> complete.set(true), this.ctx, this.masterKey);

                Awaitility.await().untilTrue(complete);
                assertEquals(0, entry.refCnt());
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
                e.printStackTrace();
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
            LedgerStorage ledgerStorage = mock(LedgerStorage.class);
            when(ledgerStorage.isFenced(anyLong())).thenReturn(true);
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

        public SetExplicitLacTest(String entryType, Object ctx, byte[] masterKey, boolean expException){
            this.entryType=entryType;
            this.ctx=ctx;
            this.masterKey=masterKey;
            this.expException=expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            final byte[] validMK="masterKey".getBytes(StandardCharsets.UTF_8);

            return Arrays.asList(new Object[][]{
                    {"NULL", null, validMK, true},
                    {"INVALID_ENTRY", new Object(), null, true},
                    {"VALID_ENTRY", null, validMK, false},

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
                e.printStackTrace();
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
                this.bookie.setExplicitLac(this.entry,
                        (rc, ledgerId, entryId, addr, ctx) -> complete.set(true), this.ctx, this.masterKey);

                Awaitility.await().untilAsserted(()->assertTrue(complete.get()));

                assertEquals(0, this.entry.refCnt());
                if(this.expException){
                    Assert.fail("Attesa exception");
                }
            } catch (Exception e) {
                e.printStackTrace();
                assertTrue(this.expException);
            }
        }
    }

    public static class SetAndGetLACTest {

        private File dir;
        private File dir2;
        private BookieImpl bookie;
        private final TmpDirs tmpDirs=new TmpDirs();

        public SetAndGetLACTest(){

        }

        @Before
        public void startup() throws Exception {
            this.dir= this.tmpDirs.createNew("bookie-impl-test", ".tmp");
            this.dir2= this.tmpDirs.createNew("bookie-impl-test", ".tmp"); //aggiunti per aumentare la percentuale su pit
            ServerConfiguration conf= TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(this.dir.toString());
            conf.setLedgerDirNames(new String[]{this.dir.getAbsolutePath()});
            conf.setForceReadOnlyBookie(true); //aggiunti per aumentare la percentuale su pit
            conf.setReadOnlyModeEnabled(true); //aggiunti per aumentare la percentuale su pit
            conf.setIndexDirName(new String[]{this.dir2.getAbsolutePath()}); //aggiunti per aumentare la percentuale su pit
            try {
                this.bookie= new TestBookieImpl(conf);
                this.bookie.start();
            } catch (Exception e) {
                e.printStackTrace();
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
        public void setAndGetLACTest() throws IOException, InterruptedException, BookieException {
            byte[] buff = "hi".getBytes();
            ByteBuf byteBuf= Unpooled.buffer(buff.length);
            byteBuf.writeBytes(buff);
            ByteBuf lac=this.bookie.createExplicitLACEntry(0L, byteBuf);
            ByteBuf lac2=Unpooled.copiedBuffer(lac);
            this.bookie.setExplicitLac(lac2, (rc, ledgerId, entryId, addr, ctx) -> {

            }, new Object(), "pass".getBytes(StandardCharsets.UTF_8));

            byte[] exp=new byte[lac.readableBytes()];
            lac.getBytes(lac.readerIndex(), exp);

            ByteBuf res=this.bookie.getExplicitLac(this.bookie.getLedgerForEntry(lac, "pass".getBytes(StandardCharsets.UTF_8)).getLedgerId());

            assertEquals(new String(exp, StandardCharsets.UTF_8),
                    new String(res.array(), StandardCharsets.UTF_8));
        }

    }

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
            conf.setForceReadOnlyBookie(true); //aggiunti per aumentare la percentuale su pit
            conf.setReadOnlyModeEnabled(true); //aggiunti per aumentare la percentuale su pit
            conf.setIndexDirName(new String[]{dir2.getAbsolutePath()}); //aggiunti per aumentare la percentuale su pit
            //conf.setBookieId(null);
            conf.setListeningInterface(null);
            conf.setAllowMultipleDirsUnderSameDiskPartition(false);
            conf.setUseHostNameAsBookieID(true);
            conf.setUseShortHostName(true);
            try {
                BookieImpl bookie = new TestBookieImpl(conf);
                bookie.start();
                fail("Il test deve fallire");
            }catch(BookieException e){
                assertTrue(true);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Si dve verificare BookieException");
            }
        }


        @Test
        public void constructorTest2() throws Exception {
            File dir = this.tmpDirs.createNew("bookie-impl-test", ".tmp");
            File dir2 = this.tmpDirs.createNew("bookie-impl-test", ".tmp"); //aggiunti per aumentare la percentuale su pit
            ServerConfiguration conf= TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(dir.toString());
            conf.setLedgerDirNames(new String[]{dir.getAbsolutePath()});
            conf.setForceReadOnlyBookie(true); //aggiunti per aumentare la percentuale su pit
            conf.setReadOnlyModeEnabled(true); //aggiunti per aumentare la percentuale su pit
            conf.setIndexDirName(new String[]{dir2.getAbsolutePath()}); //aggiunti per aumentare la percentuale su pit
            //conf.setBookieId(null);
            conf.setListeningInterface(null);
            conf.setAdvertisedAddress("127.0.0.1");
            conf.setAllowMultipleDirsUnderSameDiskPartition(false);
            try {
                BookieImpl bookie = new TestBookieImpl(conf);
                bookie.start();
                fail("Il test deve fallire");
            }catch(BookieException e){
                assertTrue(true);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Si dve verificare BookieException");
            }
        }
    }
}
