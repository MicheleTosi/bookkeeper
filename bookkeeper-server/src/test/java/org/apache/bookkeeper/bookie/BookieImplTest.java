package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(value = Enclosed.class)
public class BookieImplTest {
    private static final Logger LOG = LoggerFactory.getLogger(BookieImplTest.class);

    @RunWith(value= Parameterized.class)
    public static class CheckDirectoryStructureTest {
        String path;

        public CheckDirectoryStructureTest(String path) {
            this.path = path;
        }

        @Parameterized.Parameters
        public static Collection<Object> testCasesArgument() {
            return Arrays.asList(new Object[]{
                    null,
                    ""
            });
        }

        @Test
        public void testVerify() {
            try {
                BookieImpl.checkDirectoryStructure(new File(path));
                assertTrue(true);
            }catch (NullPointerException e){
                if (path==null){
                    assertTrue(true);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

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
        public static Collection<Object[]> testCasesArgument() {
            return Arrays.asList(new Object[][]{
                    {(new File[]{new File("bookie-impl-test/ledger-dir")}), (new File[]{new File("bookie-impl-test/ledger-dir")}), null, null, false, true, "", true},
                    {(new File[]{new File("bookie-impl-test/ledger-dir2/example.txt")}), (new File[]{new File("bookie-impl-test/ledger-dir")}), null, null, false, true, "", true},
                    {(new File[]{new File("bookie-impl-test/ledger-dir2")}), (new File[]{new File("bookie-impl-test/ledger-dir")}), null, null, true, false, "Y", true},
                    {(new File[]{new File("bookie-impl-test/ledger-dir2/example.txt")}), (new File[]{new File("bookie-impl-test/ledger-dir")}), null, null, true, false, "N", false},
                    {(new File[]{new File("bookie-impl-test/ledger-dir2/example.txt")}), (new File[]{new File("bookie-impl-test/ledger-dir")}), null, null, true, false, "E", false},
                    {(new File[]{new File("/home/bookie-impl-test")}), (new File[]{new File("bookie-impl-test/ledger-dir2")}), null, null, false, true, "", false},
                    {(new File[]{null}), (new File[]{new File("bookie-impl-test/ledger-dir2")}), null, null, false, true, "", true},
                    {null, null, null, null, false, false, "", false}
            });
        }

        @Before
        public void setup() {
            if(ledgerDirs!=null) {
                for (File path : this.journalDirs) {
                    if(path!=null && !path.exists() && !path.getName().contains("dir2") && !path.getPath().contains("home")) {
                        if (!path.mkdirs()) {
                            System.out.println(path.getPath());
                            Assert.fail("LedgerDirs non creato correttamente");
                        } else {
                            File exampleFile = new File(path, "example.txt");
                            try {
                                boolean created = exampleFile.createNewFile();
                                if (!created && !exampleFile.exists()) {
                                    Assert.fail("Impossibile creare il file: " + exampleFile.getAbsolutePath());
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                                Assert.fail("File non creato correttamente");
                            }
                        }
                    }
                }
            }
            conf = mock(ServerConfiguration.class);
            when(conf.getJournalDirs()).thenReturn(journalDirs);
            when(conf.getLedgerDirs()).thenReturn(ledgerDirs);
            when(conf.getIndexDirs()).thenReturn(indexDirs);
            when(conf.getGcEntryLogMetadataCachePath()).thenReturn(gcEntryLogMetadataCachePath);
        }

        @After
        public void cleanup() throws IOException {
            String directoryPath = "bookie-impl-test"; // percorso della directory da eliminare
            File directory = new File(directoryPath);
            if (directory.exists()) {
                // Ricorsivamente elimina la directory e tutti i suoi contenuti
                FileUtils.deleteDirectory(directory);
            }
        }

        @Test
        public void verifyTest(){
            try {
                switch (this.input){
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
                    default:
                        break;
                }
                boolean result=BookieImpl.format(conf, isInteractive, force);
                assertEquals(result, this.expectedResult);
            }catch(NullPointerException e) {
                e.printStackTrace();
                assertTrue(journalDirs == null || ledgerDirs == null || Arrays.asList(journalDirs).contains(null)
                        || Arrays.asList(ledgerDirs).contains(null));
            }
        }

    }

    @RunWith(value= Parameterized.class)
    public static class AddAndReadEntryTest{

        private TestBookieImpl bookie;
        private File dir;

        public AddAndReadEntryTest(){

        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){
            return Arrays.asList(new Object[][]{
                    {},

            });
        }

        @Before
        public void startup() throws IOException {
            this.dir= IOUtils.createTempDir("bookie-impl-test", ".tmp");
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
                FileUtils.deleteDirectory(this.dir);
            } catch (IOException e) {
                LOG.info("La directory {} non è stata eliminata", this.dir);
            }
        }

        @Test
        public void addAndReadEntryTest(){
            try {
                AtomicBoolean complete = new AtomicBoolean(false);
                final byte[] masterKey="masterKey".getBytes(StandardCharsets.UTF_8);
                byte[] buff = "Hello, world".getBytes();
                ByteBuf byteBuf= Unpooled.buffer(2*Long.BYTES+buff.length);
                byteBuf.writeLong(0L);
                byteBuf.writeLong(0L);
                byteBuf.writeBytes(buff);
                this.bookie.addEntry(byteBuf,
                        false, (rc, ledgerId, entryId, addr, ctx) -> complete.set(true), null, masterKey);

                Awaitility.await().untilAsserted(()-> assertTrue(complete.get()));

                byteBuf= Unpooled.buffer(2*Long.BYTES+buff.length);
                byteBuf.writeLong(0L);
                byteBuf.writeLong(0L);
                byteBuf.writeBytes(buff);

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

        @Test
        public void addEntryToFencedLedger() throws IOException, BookieException, InterruptedException {
            LedgerStorage ledgerStorage = mock(LedgerStorage.class);
            when(ledgerStorage.isFenced(anyLong())).thenReturn(true);
            byte[] buff = "Hello, world".getBytes();
            ByteBuf byteBuf = Unpooled.buffer(2 * Long.BYTES + buff.length);
            byteBuf.writeLong(0L);
            byteBuf.writeLong(0L);
            byteBuf.writeBytes(buff);
            this.bookie.fenceLedger(this.bookie.getLedgerForEntry(byteBuf, "pass".getBytes(StandardCharsets.UTF_8)).getLedgerId(), "pass".getBytes(StandardCharsets.UTF_8));
            try {
                this.bookie.addEntry(byteBuf, true, new BookieImpl.NopWriteCallback(), new Object(), "pass".getBytes(StandardCharsets.UTF_8));
                Assert.fail("Non deve essere possibile scrivere su un fenced ledger");
            }catch (BookieException e){
                assertTrue(true);
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

            byte[] buff = "Hello, world".getBytes();
            ByteBuf validEntry= Unpooled.buffer(2*Long.BYTES+buff.length);
            validEntry.writeLong(0L);
            validEntry.writeLong(0L);
            validEntry.writeBytes(buff);

            ByteBuf invalidEntry= Unpooled.buffer(2*Long.BYTES+buff.length);
            invalidEntry.writeLong(-1L);
            invalidEntry.writeLong(-1L);
            invalidEntry.writeBytes(buff);

            return Arrays.asList(new Object[][]{
                    {null, true, null, validMK, true},
                    {invalidEntry, false, new Object(), null, true},
                    {validEntry, true, null, validMK, false},

            });
        }

        @Before
        public void startup() throws IOException {
            this.dir= IOUtils.createTempDir("bookie-impl-test", ".tmp");
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
                FileUtils.deleteDirectory(this.dir);
            } catch (IOException e) {
                LOG.info("La directory {} non è stata eliminata", this.dir);
            }
        }

        @Test
        public void addEntryTest(){
            try {

                AtomicBoolean complete= new AtomicBoolean(false);

                this.bookie.addEntry(this.entry,
                        this.ackBeforeSync, (rc, ledgerId, entryId, addr, ctx) -> complete.set(true), this.ctx, this.masterKey);

                Awaitility.await().untilAsserted(()->assertTrue(complete.get()));
                assertEquals(0, this.entry.refCnt());
                if(this.expException){
                    Assert.fail("Attesa exception");
                }
            } catch (Exception e) {
                assertTrue(this.expException);
            }
        }

        @Test
        public void recoveryAddEntryTest(){
            try {

                AtomicBoolean complete= new AtomicBoolean(false);

                this.bookie.recoveryAddEntry(this.entry,(rc, ledgerId, entryId, addr, ctx) -> complete.set(true),
                        this.ctx, this.masterKey);

                Awaitility.await().untilAsserted(()->assertTrue(complete.get()));
                assertEquals(0, this.entry.refCnt());
                if(this.expException){
                    Assert.fail("Attesa exception");
                }
            } catch (Exception e) {
                assertTrue(this.expException);
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
        public void startup() throws IOException {
            this.dir= IOUtils.createTempDir("bookie-impl-test", ".tmp");
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
                FileUtils.deleteDirectory(this.dir);
                this.bookie.shutdown();
            } catch (IOException e) {
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
                assertTrue(this.expException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class SetAndGetLACTest {

        private File dir;
        private BookieImpl bookie;

        public SetAndGetLACTest(){

        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){
            return Arrays.asList(new Object[][]{
                    {},

            });
        }

        @Before
        public void startup() throws IOException {
            this.dir= IOUtils.createTempDir("bookie-impl-test", ".tmp");
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
                FileUtils.deleteDirectory(this.dir);
            } catch (IOException e) {
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
}
