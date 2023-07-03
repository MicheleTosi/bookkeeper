package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LedgerHandleBookieIT extends BookKeeperClusterTestCase{

    public LedgerHandleBookieIT() {
        super(3);
    }

    @Test
    public void testAddEntry() throws Exception {
        LedgerHandle lh=bkc.createLedger(3,3,BookKeeper.DigestType.CRC32, "pass".getBytes(StandardCharsets.UTF_8));
        String entry="hello";
        long entryId=lh.addEntry(entry.getBytes(StandardCharsets.UTF_8));
        byte[] buff = entry.getBytes();
        ByteBuf byteBuf= Unpooled.buffer(2*Long.BYTES+buff.length);
        byteBuf.writeBytes(buff);

        for(ServerTester server: servers){
            assertTrue(new String(server.getServer().getBookie().readEntry(lh.getId(), entryId).array(), StandardCharsets.UTF_8).contains(entry));
        }

        assertEquals("La lunghezza delle masterkey deve essere la stessa",serverByIndex(0).getBookie().getLedgerStorage().readMasterKey(lh.getId()).length, lh.getLedgerKey().length);
        assertEquals("La masterkey deve essere la stessa",new String(lh.getLedgerKey(), StandardCharsets.UTF_8), new String(serverByIndex(0).getBookie().getLedgerStorage().readMasterKey(lh.getId()), StandardCharsets.UTF_8));
        lh.close();
    }

    @Test
    public void bookieFailureTest() throws BKException, InterruptedException {
        LedgerHandle lh=bkc.createLedger(3,1,BookKeeper.DigestType.CRC32, "pass".getBytes(StandardCharsets.UTF_8));
        String entry="hello";
        long entryId=lh.addEntry(entry.getBytes(StandardCharsets.UTF_8));
        int counter=0;
        for (ServerTester server:servers) {
            try {
                if (new String(server.getServer().getBookie().readEntry(lh.getId(), entryId).array(), StandardCharsets.UTF_8).contains(entry)) {
                    server.getServer().shutdown();
                    counter++;
                }
            }catch (Exception e){
                LOG.debug("Non tengo conto dei bookie in cui è stata salvata la entry");
            }
        }
        try {
            lh.read(entryId, entryId).getEntry(entryId).getEntryBytes();
            Assert.fail("Non ci dovrebbe essere nessun bookie che mantiene i dati della entry");
        } catch (org.apache.bookkeeper.client.api.BKException e) {
            assertTrue(true);
        }
        assertEquals("Un solo server dovrebbe far parte del quorum di scrittura", 1, counter);

    }

    @Test
    public void bookieFailureTest2() throws BKException, InterruptedException {
        LedgerHandle lh=bkc.createLedger(3,2,BookKeeper.DigestType.CRC32, "pass".getBytes(StandardCharsets.UTF_8));
        String entry="hello";
        long entryId=lh.addEntry(entry.getBytes(StandardCharsets.UTF_8));
        for (ServerTester server:servers) {
            try {
                if (new String(server.getServer().getBookie().readEntry(lh.getId(), entryId).array(), StandardCharsets.UTF_8).contains(entry)) {
                    server.getServer().shutdown();
                    break;
                }
            }catch (Exception e){
                LOG.debug("Non tengo conto dei bookie in cui non è stata salvata la entry");
            }
        }
        try {
            assertEquals(new String(lh.read(entryId, entryId).getEntry(entryId).getEntryBytes(), StandardCharsets.UTF_8), entry);
        } catch (org.apache.bookkeeper.client.api.BKException e) {
            Assert.fail("Non dovrebbe fallire perché c'è almeno un bookie che ha la entry al suo interno");
        }
    }

    @Test
    public void addEntryToFenceLedger() throws Exception {
        LedgerHandle lh=bkc.createLedger(3,2,BookKeeper.DigestType.CRC32, "pass".getBytes(StandardCharsets.UTF_8));
        serverByIndex(0).getBookie().fenceLedger(lh.getId(), lh.getLedgerKey()).get();
        try{
            lh.addEntry("casa".getBytes(StandardCharsets.UTF_8));
            Assert.fail("Non deve essere possibile scrivere su un fenced ledger tramite add entry");
        }catch (InterruptedException | BKException e){
            assertTrue(true);
        }
        lh.close();
    }
}

