package org.apache.bookkeeper.client;

import org.apache.bookkeeper.bookie.BookKeeperClusterTestCase;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.utils.DeleteTempFiles;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class LedgerHandleTest extends BookKeeperClusterTestCase {

        private final Object ctx;
        private LedgerHandle lh;
        private final long entryId;
        private final long timeOutInMillis;
        private final boolean parallel;
        private final boolean isClosed;
        private final boolean notNullCallback;

        public LedgerHandleTest(long entryId, long timeOutInMillis, boolean notNullCallback, boolean parallel, Object ctx, boolean isClosed){
            super(3);
            this.entryId =entryId;
            this.timeOutInMillis=timeOutInMillis;
            this.notNullCallback=notNullCallback;
            this.parallel=parallel;
            this.ctx=ctx;
            this.isClosed=isClosed;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {

            return Arrays.asList(new Object[][]{
                    {-1, -1, true, true, new Object(), true},
                    {0, 0, true, false, null, false},
                    {1, 1, false, true, new Object(), true},
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
            lh= bkc.createLedger(BookKeeper.DigestType.CRC32, "ciao".getBytes());
            long entryId=lh.addEntry("hello".getBytes(), 0, "hello".length()-2);
            assertEquals(0L, entryId);
        }

}
