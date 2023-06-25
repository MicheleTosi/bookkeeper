package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.bookie.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LedgerHandleTest extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(LedgerHandleTest.class);
    private LedgerHandle lh;
    private LedgerHandle lh2;
    private BookKeeper bk;
    public LedgerHandleTest(){
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
        lh2 = bookKeeper.createLedger(BookKeeper.DigestType.CRC32, "password2".getBytes());
    }

    @After
    public void tearDown(){
        try {
            lh.close();
            lh2.close();
        } catch (InterruptedException | BKException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAddEntry() throws BKException, InterruptedException {
        lh.addEntry("ciao".getBytes(StandardCharsets.UTF_8));
        assertEquals(lh.getLength(), "ciao".getBytes(StandardCharsets.UTF_8).length);
        try {
            lh.addEntry(null);
        }catch(NullPointerException e){
            assertTrue(true);
        }
    }

    @Test
    public void testSynchronousAddEntry() throws BKException, InterruptedException {
        try{
            lh.addEntry(0, "hello".getBytes(StandardCharsets.UTF_8), 0, "hello".getBytes(StandardCharsets.UTF_8).length);
            Assert.fail("To use this feature Ledger must be created with createLedgerAdv() interface");
        }catch (Exception e){
            assertTrue(true);
        }
        //Map<BookieId, BookieInfoReader.BookieInfo> bookies = bk.getBookieInfo();

        /*for (BookieId id: bookies.keySet()) {

        }*/
//        new BookieImpl().fenceLedger(lh.getId(), lh.getLedgerKey());
    }

}
