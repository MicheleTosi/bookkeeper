package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.bookie.BookKeeperClusterTestCase;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.utils.DeleteTempFiles;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.bookkeeper.utils.EntryBuilder.getInvalidEntry;
import static org.apache.bookkeeper.utils.EntryBuilder.getValidEntry;
import static org.junit.Assert.*;

@RunWith(Enclosed.class)
public class LedgerHandleTests {
    static final Logger LOG = LoggerFactory.getLogger(LedgerHandleTests.class);

    @Rule
    public Timeout globalTimeout=Timeout.seconds(10L);

    @RunWith(Parameterized.class)
    public static class AddEntryTest extends BookKeeperClusterTestCase {
        private LedgerHandle lh;
        private final byte[] data;
        private BookKeeper bookKeeper;
        private final boolean expException;
        public AddEntryTest(byte[] data, boolean expException){
            super(5);
            this.data=data;
            this.expException=expException;
        }


        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            return Arrays.asList(new Object[][]{
                    {"newEntry".getBytes(), false},
                    {null, true},
                    {"".getBytes(), false},
            });
        }

        @Before
        public void setup() throws Exception {
            // Inizializza BookKeeper utilizzando la configurazione
            ClientConfiguration bkConf = TestBKConfiguration.newClientConfiguration();
            bkConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
            bookKeeper = new BookKeeper(bkConf);

            // Crea un nuovo ledger di test
            lh = bookKeeper.createLedger(BookKeeper.DigestType.CRC32, "password".getBytes());
        }

        @After
        public void tearDown(){
            try {
                DeleteTempFiles.deleteTempFiles();
                lh.close();
            } catch (InterruptedException | BKException e) {
                //non mi interessa se ci sono errori nella chiusura di lh
            }
        }

        @Test
        public void testAddEntry() {
            try {
                long entryId=lh.addEntry(this.data);
                assertEquals(0, entryId);
                assertEquals(lh.getLength(), this.data.length);
            }catch(BKException | InterruptedException | NullPointerException e){
                assertTrue(this.expException);
            }
        }

    }

    public static class AddEntry2Test extends BookKeeperClusterTestCase{
        private LedgerHandle lh;
        public AddEntry2Test(){
            super(5);
        }
        @Before
        public void setup() throws Exception {
            // Inizializza BookKeeper utilizzando la configurazione
            ClientConfiguration bkConf = TestBKConfiguration.newClientConfiguration();
            bkConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
            BookKeeper bookKeeper = new BookKeeper(bkConf);

            // Crea un nuovo ledger di test
            lh = bookKeeper.createLedger(BookKeeper.DigestType.CRC32, "password".getBytes());
        }

        @After
        public void tearDown(){
            try {
                DeleteTempFiles.deleteTempFiles();
                lh.close();
            } catch (InterruptedException | BKException e) {
                //non mi interessa se ci sono errori nella chiusura di lh
            }
        }
        @Test
        public void testAddEntry2() {
            try {
                byte[] data1="data1".getBytes(StandardCharsets.UTF_8);
                byte[] data2="data2".getBytes(StandardCharsets.UTF_8);
                long entryId=lh.addEntry(data1);
                long entryId2=lh.addEntry(data2);
                assertEquals(0L, entryId);
                assertEquals(1L, entryId2);
                assertEquals(data1.length+data2.length,lh.getLength());
                assertEquals(1L, lh.getLastAddConfirmed());
            }catch(Exception e){
                fail("Errore non atteso");
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class AppendAsyncTest extends BookKeeperClusterTestCase{
        private LedgerHandle lh;
        private final ByteBuf data;
        private final boolean expException;

        public AppendAsyncTest(ByteBuf data, boolean expException){
            super(3);
            this.data=data;
            this.expException=expException;
        }


        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            return Arrays.asList(new Object[][]{
                    {null, true},
                    {getValidEntry(), false},
            });
        }

        @Before
        public void setup() throws Exception {
            // Inizializza BookKeeper utilizzando la configurazione
            ClientConfiguration bkConf = TestBKConfiguration.newClientConfiguration();
            bkConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
            bkConf.setWaitTimeoutOnBackpressureMillis(1L); //aumentare coverage PIT
            BookKeeper bookKeeper = new BookKeeper(bkConf);

            // Crea un nuovo ledger di test
            lh = bookKeeper.createLedger(BookKeeper.DigestType.CRC32, "password".getBytes());
        }

        @After
        public void tearDown(){
            try {
                DeleteTempFiles.deleteTempFiles();
                lh.close();
            } catch (InterruptedException | BKException e) {
                //non mi interessa se ci sono errori nella chiusura di lh
            }
        }
        @Test
        public void appendAsyncTest(){
            try {
                Long entryId=lh.appendAsync(this.data).get();
                assertEquals(Long.valueOf(0), entryId);
                assertEquals(0, this.data.refCnt());
            }catch(Exception e){
                assertTrue(this.expException);
            }
        }

    }

    public static class SynchronousAddEntryTest extends BookKeeperClusterTestCase {

        private LedgerHandle lh;

        public SynchronousAddEntryTest() {
            super(3);
        }

        @Before
        public void setup() throws Exception {
            // Inizializza BookKeeper utilizzando la configurazione
            ClientConfiguration bkConf = TestBKConfiguration.newClientConfiguration();
            bkConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
            BookKeeper bookKeeper = new BookKeeper(bkConf);

            // Crea un nuovo ledger di test
            lh = bookKeeper.createLedger(BookKeeper.DigestType.CRC32, "password".getBytes());
        }

        @After
        public void tearDown() {
            try {
                DeleteTempFiles.deleteTempFiles();
                lh.close();
            } catch (InterruptedException | BKException e) {
                throw new RuntimeException(e);
            }
        }

        @Test
        public void testSynchronousAddEntry() {
            try {
                lh.addEntry(0, "hello".getBytes(StandardCharsets.UTF_8), 0, "hello".getBytes(StandardCharsets.UTF_8).length);
                fail("To use this feature Ledger must be created with createLedgerAdv() interface");
            } catch (Exception e) {
                assertTrue(true);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class AddAndReadEntryTest extends BookKeeperClusterTestCase {
        private BookKeeper bk;
        private final int firstEntry;
        private final int lastEntry;
        private final Object ctx;
        private final boolean clientClosed;
        private final boolean expException;
        private LedgerHandle lh;

        public AddAndReadEntryTest(int firstEntry, int lastEntry, Object ctx, boolean clientClosed, boolean expException) {
            super(3);
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.ctx = ctx;
            this.clientClosed=clientClosed;
            this.expException=expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {

            return Arrays.asList(new Object[][]{
                    {-1, -2, new Object(), false, true},
                    {-1, -1, null, false, true},
                    {-1, 0, new Object(), false, true},
                    {0, -1, null, false, true},
                    {0, 0, new Object(), false, false},
                    {0, 0, null, true, true},
                    {0, 1, null, false, true},
                    {1, 0, new Object(), false, true},
                    {1, 1, null, false, true},
                    {1, 2, new Object(), false, true},
            });
        }

        @Before
        public void setup() throws Exception {
            // Inizializza BookKeeper utilizzando la configurazione
            ClientConfiguration bkConf = TestBKConfiguration.newClientConfiguration();
            bkConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
            bk = new BookKeeper(bkConf);

            // Crea un nuovo ledger di test
            lh = bk.createLedger(BookKeeper.DigestType.CRC32, "password".getBytes());
        }

        @After
        public void tearDown() {
            try {
                DeleteTempFiles.deleteTempFiles();
                lh.close();
            } catch (InterruptedException | BKException e) {
                // non fa nulla se fallisce
            }
        }

        @Test
        public void addAndAsyncReadEntryTest() throws BKException, InterruptedException {
            AtomicInteger retCode = new AtomicInteger();
            AtomicBoolean complete = new AtomicBoolean(false);
            AtomicReference<Enumeration<LedgerEntry>> sequence = new AtomicReference<>();
            String data = "entry";
            lh.addEntry(data.getBytes(StandardCharsets.UTF_8));
            if(this.clientClosed){
                bk.close(); //aggiunto per aumentare la coverage su PIT
            }
            lh.asyncReadEntries(this.firstEntry, this.lastEntry, new AsyncCallback.ReadCallback() {
                @Override
                public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                    retCode.set(rc);
                    sequence.set(seq);
                    complete.set(true);
                }
            }, this.ctx);
            Awaitility.await().untilTrue(complete);
            if(retCode.get()==BKException.Code.OK) {
                assertEquals(data, new String(sequence.get().nextElement().getEntry(), StandardCharsets.UTF_8));
                assertFalse(expException);
            }else{
                assertTrue(expException);
            }
        }

        @Test
        public void addAndReadEntryTest() throws InterruptedException {
            try {
                String data = "entry";
                lh.addEntry(data.getBytes(StandardCharsets.UTF_8));
                if(this.clientClosed){
                bk.close(); //aggiunto per aumentare la coverage su PIT
                }
                Enumeration<LedgerEntry> entries = lh.readEntries(this.firstEntry, this.lastEntry);
                assertEquals(data, new String(entries.nextElement().getEntry(), StandardCharsets.UTF_8));
            } catch (BKException e) {
                assertTrue(this.expException);
            }
        }

        @Test
        public void addAndReadAsyncEntryTest() throws InterruptedException {
            try {
                String data = "entry";
                lh.addEntry(data.getBytes(StandardCharsets.UTF_8));
                if(this.clientClosed){
                    bk.close(); //aggiunto per aumentare la coverage su PIT
                }
                LedgerEntries entries = lh.readAsync(this.firstEntry, this.lastEntry).get();
                assertEquals(data, new String(entries.getEntry(this.firstEntry).getEntryBytes(), StandardCharsets.UTF_8));
            } catch (BKException | ExecutionException e) {
                assertTrue(this.expException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class AddAndAsyncReadLastEntryTest extends BookKeeperClusterTestCase {
        private final Object ctx;
        private final boolean addedEntry;
        private final boolean expException;
        private LedgerHandle lh;

        public AddAndAsyncReadLastEntryTest(Object ctx, boolean addedEntry, boolean expException) {
            super(3);
            this.ctx = ctx;
            this.addedEntry=addedEntry;
            this.expException=expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {

            return Arrays.asList(new Object[][]{
                    {new Object(), true, false},
                    {null, false, true},
            });
        }

        @Before
        public void setup() throws Exception {
            // Inizializza BookKeeper utilizzando la configurazione
            ClientConfiguration bkConf = TestBKConfiguration.newClientConfiguration();
            bkConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
            bkConf.setStickyReadsEnabled(true); //aumentare coverage PIT
            BookKeeper bk = new BookKeeper(bkConf);

            // Crea un nuovo ledger di test
            lh = bk.createLedger(3, 3,BookKeeper.DigestType.CRC32, "password".getBytes());
        }

        @After
        public void tearDown() {
            try {
                DeleteTempFiles.deleteTempFiles();
                lh.close();
            } catch (InterruptedException | BKException e) {
                // non fa nulla se fallisce
            }
        }
        @Test
        public void addAndAsyncReadLastEntryTest() throws InterruptedException, BKException {

            String data = "entry";
            AtomicReference<Enumeration<LedgerEntry>> sequence = new AtomicReference<>();
            AtomicBoolean complete=new AtomicBoolean(false);
            AtomicLong retCode=new AtomicLong();
            if(addedEntry) {
                lh.addEntry(data.getBytes(StandardCharsets.UTF_8));
            }
            lh.asyncReadLastEntry(new AsyncCallback.ReadCallback() {
                @Override
                public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                    retCode.set(rc);
                    sequence.set(seq);
                    complete.set(true);
                }
            }, this.ctx);
            Awaitility.await().untilTrue(complete);
            if(retCode.get()==BKException.Code.OK) {
                assertEquals(data, new String(sequence.get().nextElement().getEntry(), StandardCharsets.UTF_8));
            }else{
                assertTrue(this.expException);
            }

        }

    }

    @RunWith(Parameterized.class)
    public static class AddAndReadUnconfirmedEntriesTest extends BookKeeperClusterTestCase {

        private final int firstEntry;
        private final int lastEntry;
        private LedgerHandle lh;

        public AddAndReadUnconfirmedEntriesTest(int firstEntry, int lastEntry) {
            super(3);
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {

            return Arrays.asList(new Object[][]{
                    {-1, -2},
                    {-1, -1},
                    {-1, 0},
                    {0, -1},
                    {0, 0},
                    {0, 1},
                    {1, 0},
                    {1, 1},
                    {1, 2},
            });
        }

        @Before
        public void setup() throws Exception {
            // Inizializza BookKeeper utilizzando la configurazione
            ClientConfiguration bkConf = TestBKConfiguration.newClientConfiguration();
            bkConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
            BookKeeper bk = new BookKeeper(bkConf);

            // Crea un nuovo ledger di test
            lh = bk.createLedger(BookKeeper.DigestType.CRC32, "password".getBytes());
        }

        @After
        public void tearDown() {
            try {
                DeleteTempFiles.deleteTempFiles();
                lh.close();
            } catch (InterruptedException | BKException e) {
                // non fa nulla se fallisce
            }
        }
        @Test
        public void addAndReadUnconfirmedEntriesTest() throws InterruptedException {
            try {
                String data = "entry";
                lh.addEntry(data.getBytes(StandardCharsets.UTF_8));
                Enumeration<LedgerEntry> entries = lh.readUnconfirmedEntries(this.firstEntry, this.lastEntry);
                assertEquals(data, new String(entries.nextElement().getEntry(), StandardCharsets.UTF_8));
            }catch(BKException e){
                if (this.firstEntry==this.lastEntry && this.firstEntry==0){
                    fail("Errore non atteso");
                }else{
                    assertTrue(true);
                }
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class ReadLastAddConfirmedAndEntryAsyncTest extends BookKeeperClusterTestCase{

        private final Object ctx;
        private LedgerHandle lh;
        private final long entryId;
        private final long timeOutInMillis;
        private final boolean parallel;
        private final boolean isClosed;

        public ReadLastAddConfirmedAndEntryAsyncTest(long entryId, long timeOutInMillis, boolean parallel, Object ctx, boolean isClosed){
            super(3);
            this.entryId =entryId;
            this.timeOutInMillis=timeOutInMillis;
            this.parallel=parallel;
            this.ctx=ctx;
            this.isClosed=isClosed;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {

            return Arrays.asList(new Object[][]{
                    {-1, -1, true, new Object(), true},
                    {0, 0, false, null, false},
                    {0, 0, false, null, true},
                    {1, 1, true, new Object(), true},
            });
        }

        @Before
        public void setup() throws Exception {
            // Inizializza BookKeeper utilizzando la configurazione
            ClientConfiguration bkConf = TestBKConfiguration.newClientConfiguration();
            bkConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
            BookKeeper bk = new BookKeeper(bkConf);

            // Crea un nuovo ledger di test
            lh = bk.createLedger(BookKeeper.DigestType.CRC32, "password".getBytes());
        }

        @After
        public void tearDown() {
            try {
                DeleteTempFiles.deleteTempFiles();
                lh.close();
            } catch (InterruptedException | BKException e) {
                // non fa nulla se fallisce
            }
        }
        @Test
        public void readLastConfirmedAndEntryTest() throws InterruptedException, BKException {
            String data = "entry";
            AtomicInteger rcAtomic=new AtomicInteger();
            AtomicLong lastConf=new AtomicLong();
            AtomicBoolean complete=new AtomicBoolean(false);
            AtomicReference<LedgerEntry> entryAtomicReference=new AtomicReference<>();
            long entryId=lh.addEntry(data.getBytes(StandardCharsets.UTF_8));
            if(isClosed){
                lh.close();
            }
            lh.asyncReadLastConfirmedAndEntry(this.entryId, this.timeOutInMillis, this.parallel, new AsyncCallback.ReadLastConfirmedAndEntryCallback() {
                @Override
                public void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx) {
                    rcAtomic.set(rc);
                    lastConf.set(lastConfirmed);
                    entryAtomicReference.set(entry);
                    complete.set(true);
                }
            }, this.ctx);
            Awaitility.await().untilTrue(complete);
            if(rcAtomic.get()==BKException.Code.OK){
                assertEquals(entryId, lastConf.get());
                if(this.entryId==lh.getLastAddConfirmed()) {
                    assertEquals(data, new String(entryAtomicReference.get().getEntry(), StandardCharsets.UTF_8));
                }else{
                    assertNull(entryAtomicReference.get());
                }
            }else{
                assertEquals(-1L, lastConf.get());
            }
        }

    }

    @RunWith(Parameterized.class)
    public static class AsyncReadLastConfirmedTest extends BookKeeperClusterTestCase{

        private final Object ctx;
        private final boolean useV2WireProtocol;
        private final boolean isClosed;
        private LedgerHandle lh;
        private BookKeeper bk;

        public AsyncReadLastConfirmedTest(Object ctx, boolean useV2WireProtocol, boolean isClosed){
            super(3);
            this.ctx=ctx;
            this.useV2WireProtocol=useV2WireProtocol; //aggiunto per PIT
            this.isClosed=isClosed;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {

            return Arrays.asList(new Object[][]{
                    {new Object(), false, true},
                    {null, true, true},
                    {new Object(), false, false},
                    {null, true, false},
            });
        }

        @Before
        public void setup() throws Exception {
            // Inizializza BookKeeper utilizzando la configurazione
            ClientConfiguration bkConf = TestBKConfiguration.newClientConfiguration();
            bkConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
            bkConf.setExplictLacInterval(0);
            bkConf.setUseV2WireProtocol(useV2WireProtocol);
            bk = new BookKeeper(bkConf);

            // Crea un nuovo ledger di test
            lh = bk.createLedger(BookKeeper.DigestType.CRC32, "password".getBytes());
        }

        @After
        public void tearDown() {
            try {
                DeleteTempFiles.deleteTempFiles();
                lh.close();
            } catch (InterruptedException | BKException e) {
                // non fa nulla se fallisce
            }
        }
        @Test
        public void readLastConfirmedTest() throws InterruptedException, BKException, ExecutionException {
            String data = "entry";
            AtomicInteger rcAtomic=new AtomicInteger();
            AtomicLong entry=new AtomicLong();
            AtomicLong lastConf=new AtomicLong();
            AtomicBoolean complete=new AtomicBoolean(false);

            long entryId=lh.addEntry(data.getBytes(StandardCharsets.UTF_8));
            lh.asyncAddEntry(data.getBytes(), new AsyncCallback.AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    entry.set(entryId);
                    complete.set(true);
                }
            }, null);
            Awaitility.await().untilTrue(complete);
            if(isClosed){
                lh.close();
            }
            complete.set(false);
            lh.asyncReadLastConfirmed(new AsyncCallback.ReadLastConfirmedCallback() {
                @Override
                public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                    rcAtomic.set(rc);
                    lastConf.set(lastConfirmed);
                    complete.set(true);
                }
            }, this.ctx);
            Awaitility.await().untilTrue(complete);
            if(!isClosed) {
                if (rcAtomic.get() == BKException.Code.OK) {
                    assertEquals(entryId, lastConf.get());
                } else {
                    assertEquals(-1L, lastConf.get());
                }
            }else{
                if (rcAtomic.get() == BKException.Code.OK) {
                    assertEquals(lh.getLedgerMetadata().getLastEntryId(), lastConf.get());
                } else {
                    assertEquals(-1L, lastConf.get());
                }
            }
        }

    }

}
